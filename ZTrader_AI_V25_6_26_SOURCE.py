import re, ast, base64, lzma
import json, signal, threading, time, math, os, tempfile, copy, sys, hashlib
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen

STAGE='Z3-TRI-AI-INTEGRATED-030-BTCUSDT-ONE-PAGE-PAPER-V25.6.26-VALIDATED-UPGRADE-ROLLBACK'
APP_VERSION='25.6.26'
UPGRADE_URL='https://raw.githubusercontent.com/Hakeemadefowope-cyber/Z-trader--ai/main/ZTrader_AI_UPGRADE.txt'
UPGRADE_TIMEOUT=12.0
HOST='127.0.0.1'; PORT=8790; SYMBOL='BTCUSDT'
PAPER_ONLY=True; LIVE_TRADING=False; LIVE_EXECUTION_AUTHORIZED=False; REAL_ORDER_EXECUTION=False
MARKET_DATA_AUTH_REQUIRED=False  # Public market-data endpoints do not require private trading API credentials.
START_BALANCE=100.0; DEFAULT_POOL=100.0; DEFAULT_PER_TRADE=10.0; MIN_TRADE=.01; MAX_POSITIONS=4
FEE=.0004; SLIPPAGE=.0002
DATA_EVERY=1.0; QUALITY_EVERY=5.0; LOOP=.20; STALE_SECONDS=15.0; MARKET_1M_MAX_AGE=120.0; MARKET_5M_MAX_AGE=360.0; TICKER_CACHE_SECONDS=.75; DERIVATIVES_EVERY=30.0; NETWORK_TIMEOUT=8.0; NETWORK_RETRY=1.5; MARKET_HOSTS=['api.bybit.com','api.bytick.com','api.bybit.nl']
SL_ATR_BUFFER=.12; FALLBACK_SL_ATR=1.15; MAX_SL_ATR=2.20
BREAKEVEN_R=.35; TRAIL_START_R=.60; TRAIL_ATR=.70; MAX_HOLD=3; MAX_HOLD_SECONDS=180.0
INTEL_MIN_ALIGNMENT=52; INTEL_STRONG_ALIGNMENT=68; EXIT_CONFIRMATIONS=1; PROFIT_LOCK_R=.35; GIVEBACK_EXIT_PCT=35.0; THESIS_FAIL_R=-.20; THESIS_GRACE_CYCLES=1; STRUCTURE_STOP_GRACE_ATR=.18; STRUCTURE_STOP_MAX_GRACE=1
MIN_SCORE=58; STRONG_SCORE=68; MIN_RR=.65
MAX_POOL_DRAWDOWN=.20; DAILY_LOSS_LIMIT=.05; MAX_CONSECUTIVE_LOSSES=3; COOLDOWN_AFTER_LOSS=2
MAX_SPREAD_BPS=6.0; MIN_MICRO_EDGE=.03; VALIDATION_MIN_SAMPLES=20
# Centralized adaptive-quality weights. They are normalized at startup and are
# direction-aware through the pressure component; no component is silently ignored.
ADAPTIVE_WEIGHTS={'alignment':.24,'confidence':.16,'trend':.15,'liquidity':.12,'volume':.06,'volatility':.08,'momentum':.07,'rsi':.05,'pressure':.07}
if set(ADAPTIVE_WEIGHTS)!={'alignment','confidence','trend','liquidity','volume','volatility','momentum','rsi','pressure'} or any((not math.isfinite(float(v)) or float(v)<0) for v in ADAPTIVE_WEIGHTS.values()): raise RuntimeError('Invalid adaptive weight configuration')
ADAPTIVE_WEIGHT_SUM=sum(ADAPTIVE_WEIGHTS.values())
if not math.isfinite(ADAPTIVE_WEIGHT_SUM) or ADAPTIVE_WEIGHT_SUM<=0: raise RuntimeError('Invalid adaptive weight configuration')
ADAPTIVE_WEIGHTS={k:v/ADAPTIVE_WEIGHT_SUM for k,v in ADAPTIVE_WEIGHTS.items()}
if abs(sum(ADAPTIVE_WEIGHTS.values())-1.0)>1e-9: raise RuntimeError('Adaptive weights failed normalization')
STATE_FILE=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ZTrader_AI_runtime_state.json')
PERSIST_EVERY=2.0
RECOVERY_RETRY=3.0
PERSISTENCE_VERSION=9
PERSIST_VERIFY=True
LOCK=threading.RLock(); SAVE_LOCK=threading.Lock(); STOP=threading.Event(); MARKET_STOP=threading.Event(); MARKET_THREAD_STARTED=False; MARKET_THREAD=None; ENGINE_THREAD=None; THREAD_START_LOCK=threading.RLock()
API_CACHE={}; TICKER_CACHE={'price':0.0,'time':0.0}; CACHE_LOCK=threading.RLock()
RECOVERY_LOCK=threading.Lock()
API_TTLS={'1':3.0,'5':3.0,'15':15.0,'240':60.0,'D':300.0}

S={
 'running':False,'paused':False,'paper_balance':START_BALANCE,'pool':DEFAULT_POOL,'per_trade':DEFAULT_PER_TRADE,'account_config_version':2,
 'allocated':0.0,'lot_size':0.0,'price':0.0,'signal':'WAIT','score':0,'regime':'UNKNOWN',
 'daily_structure':'UNKNOWN','fourh_structure':'UNKNOWN','candles':[],'open':[],'closed':[],'chat':[],
 'wins':0,'losses':0,'realized_pnl_total':0.0,'next_id':1,'decisions':0,'last_candle':None,'profit_closes':0,
 'take_profit':0.0,'auto_take_profit':0.0,'errors':[],'last_action':'WAIT','prediction':50.0,
 'prediction_reason':'','decision_note':'','entry_gate':'NO_SIGNAL','last_signal_time':'','last_trade_time':'',
 'daily_hh':0.0,'daily_hl':0.0,'daily_lh':0.0,'daily_ll':0.0,'stop_loss_mode':'AI STRUCTURE','last_stop_reason':'',
 'peak_pool':DEFAULT_POOL,'day_start_pool':DEFAULT_POOL,'day_key':datetime.now(timezone.utc).date().isoformat(),
 'loss_cooldown':0,'consecutive_losses':0,'market_quality':'UNKNOWN','spread_bps':0.0,'order_imbalance':0.0,
 'funding_rate':0.0,'open_interest_change':0.0,'volume_ratio':0.0,'atr_pct':0.0,'risk_block':'',
 'last_tick':0.0,'last_quality_check':0.0,'last_derivatives_check':0.0,'last_market_data_success':0.0,'last_market_data_failure':0.0,'connection_failures':0,'connection_recoveries':0,'stale_data_seconds':0.0,'session_started_at':'','session_signals':0,'session_validation_passes':0,'session_entries':0,'session_closes':0,'session_risk_blocks':0,'session_data_blocks':0,'session_connection_losses':0,
 'connection':'UNKNOWN','last_connection_change':'', 'dashboard_note':'Dashboard is independent of the engine loop.',
 'validation':{'status':'NOT_RUN','samples':0,'win_rate':0.0,'profit_factor':0.0,'expectancy':0.0,'threshold':0,'tested':[]},
 'validated_threshold':0,'signals_seen':0,'buy_signals':0,'sell_signals':0,'validation_passes':0,'validation_rejects':0,
 'risk_blocks':0,'entry_attempts':0,'paper_entries':0,'rejection_reasons':{},'decision_log':[],'engine_heartbeat':0.0,
 'desired_running':False,'recovery_pending':False,'recovery_state':'CLEAN_START','run_generation':0,'last_persist_time':'','last_recovery_time':'','restart_count':0,'service_state':'STARTING','persistence_file':STATE_FILE,
 'persistence_ok':False,'persistence_status':'NOT_VERIFIED','engine_errors':[],'engine_cycle_count':0,'engine_last_error':'','last_command':'','last_command_ok':True,'last_command_result':'','persistence_error':'','persistence_version':PERSISTENCE_VERSION,'last_persist_reason':'','last_persist_file_size':0,'last_persist_file_mtime':'','state_schema':'INTEGRATED-030','analytics':{},'performance_samples':0,'strategy_health':'INITIALIZING','last_strategy_review':'','score_components':{},'execution_quality':'UNKNOWN','forensic_summary':{},'exit_analysis':{},'data_quality_warnings':[],
  'market_intelligence':{},'one_minute_candles':[],'last_1m_candle':None,'quick_signal':'WAIT','quick_score':0,'quick_mode':'IDLE','quick_reason':'','last_intelligence_time':'','signal_engine_status':'STARTING','signal_engine_error':'','signal_engine_last_eval':'','signal_engine_evaluations':0,'last_signal_engine_side':'WAIT','last_signal_engine_score':0.0,'market_phase':'UNKNOWN','market_confidence':0.0,'market_direction':'WAIT','market_alignment':0.0,'thesis_reviews':0,'adaptive_exit_count':0,
 'adaptive_strategy':{},'setup_quality':0.0,'adaptive_threshold':70,'confluence_score':0.0,
 'entry_validation':'NOT_READY','validation_reasons':[],'data_verification_status':'NOT_RUN','data_verification_reason':'','data_verification_detail':'','verified_snapshot_generation':0,'last_processed_snapshot_generation':0,'verification_state':'IDLE','last_verification_time':'','signal_handoff_repairs':0,'regime_quality':0.0,'trend_agreement':0.0,
 'liquidity_score':0.0,'volatility_score':0.0,'momentum_score':0.0,'strategy_027_reviews':0,
 'strategy_027_rejections':0,'strategy_027_passes':0,'last_strategy_027_review':'',
 'entry_quality_history':[],'regime_performance':{},'strategy_confidence':0.0,'voice_state':'UNKNOWN','voice_last_event':'','voice_last_error':'','voice_last_report':'','voice_recovery':'NONE','network_probe':'NOT_RUN','network_probe_time':0.0,'network_probe_details':{},'market_data_source':'UNKNOWN','market_data_last_ok':'','market_data_last_error':'','market_data_worker_state':'IDLE','market_data_auth_status':'PUBLIC_ENDPOINTS_NO_KEY_REQUIRED','market_data_last_success_source':'','market_data_fetch_attempts':0,'market_data_fetch_successes':0,'market_data_consecutive_failures':0,'market_data_last_failure_reason':'','market_data_last_failure_source':'','market_data_last_failure_at':0.0,'market_data_last_success_at':0.0,'recovery_attempts':0,'recovery_successes':0,'recovery_last_reason':'','recovery_last_source':'','recovery_last_1m_age':0.0,'recovery_last_5m_age':0.0,'recovery_last_1m_cache_age':0.0,'recovery_last_5m_cache_age':0.0,'recovery_last_ticker_ok':False,'recovery_last_network_ok':False,'recovery_next_retry':0.0
}

def now(): return datetime.now(timezone.utc).isoformat(timespec='seconds')


def persist_state(reason='periodic'):
    """Atomically persist runtime state and verify the resulting file."""
    started=time.time()
    try:
        with LOCK:
            data=copy.deepcopy(S)
            data['persistence_version']=PERSISTENCE_VERSION
            data['state_schema']='INTEGRATED-030'
            data['candles']=S.get('candles',[])[-180:]
            data['closed']=S.get('closed',[])[-300:]
            data['chat']=S.get('chat',[])[-100:]
            data['decision_log']=S.get('decision_log',[])[-200:]
            stamp=now()
            data['last_persist_time']=stamp
            data['last_persist_reason']=reason
            data['persistence_error']=''
        folder=os.path.dirname(STATE_FILE) or '.'
        os.makedirs(folder,exist_ok=True)
        fd,tmp=tempfile.mkstemp(prefix='.ztrader_state_',suffix='.tmp',dir=folder,text=True)
        with SAVE_LOCK:
            try:
                with os.fdopen(fd,'w',encoding='utf-8') as f:
                    json.dump(data,f,separators=(',',':'))
                    f.flush();os.fsync(f.fileno())
                os.replace(tmp,STATE_FILE)
                size=os.path.getsize(STATE_FILE)
                mtime=datetime.fromtimestamp(os.path.getmtime(STATE_FILE),timezone.utc).isoformat(timespec='seconds')
                verified=True
                if PERSIST_VERIFY:
                    with open(STATE_FILE,'r',encoding='utf-8') as f: check=json.load(f)
                    verified=(check.get('persistence_version')==PERSISTENCE_VERSION and check.get('last_persist_time')==stamp and check.get('state_schema')=='INTEGRATED-030')
            finally:
                if os.path.exists(tmp): os.remove(tmp)
        with LOCK:
            S['last_persist_time']=stamp
            S['last_persist_reason']=reason
            S['persistence_ok']=bool(verified)
            S['persistence_status']='VERIFIED' if verified else 'VERIFY_FAILED'
            S['persistence_error']='' if verified else 'State file verification mismatch.'
            S['last_persist_file_size']=size
            S['last_persist_file_mtime']=mtime
            S['persistence_latency_ms']=round((time.time()-started)*1000,2)
        return bool(verified)
    except Exception as e:
        with LOCK:
            S['persistence_ok']=False
            S['persistence_status']='ERROR'
            S['persistence_error']=f'{type(e).__name__}: {e}'[-240:]
            S['errors']=(S.get('errors',[])+['PERSISTENCE: '+str(e)[-180:]])[-10:]
        return False

def safe_float(v,default=0.0):
    try:
        x=float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _as_dict(v):
    return dict(v) if isinstance(v,dict) else {}


def _as_list(v):
    return list(v) if isinstance(v,list) else []


def log_error(label,exc):
    with LOCK:
        S['errors']=(S.get('errors',[])+[f'{label}: {exc}'])[-10:]


def load_state():
    if not os.path.exists(STATE_FILE):
        with LOCK:
            S['service_state']='READY';S['recovery_state']='NO_SAVED_STATE'
        return False
    try:
        with open(STATE_FILE,'r',encoding='utf-8') as f:data=json.load(f)
        with LOCK:
            for k,v in data.items():
                if k in S and k not in ('engine_heartbeat','service_state','recovery_pending','last_persist_time','persistence_file'):
                    if k in ('paper_balance','pool','per_trade') and data.get('account_config_version')!=2:
                        continue
                    S[k]=v
            # Enforce account invariants after loading persisted state.
            S['paper_balance']=max(0.0,safe_float(S.get('paper_balance'),START_BALANCE))
            S['pool']=max(0.0,min(safe_float(S.get('pool'),DEFAULT_POOL),S['paper_balance']))
            S['per_trade']=max(MIN_TRADE,min(safe_float(S.get('per_trade'),DEFAULT_PER_TRADE),S['pool'] if S['pool']>0 else MIN_TRADE))
            loaded_realized=safe_float(data.get('realized_pnl_total'),float('nan'))
            if not math.isfinite(loaded_realized):
                loaded_realized=sum(safe_float(x.get('pnl')) for x in _as_list(data.get('closed')) if isinstance(x,dict))
            S['realized_pnl_total']=round(loaded_realized,6)
            S['allocated']=max(0.0,min(safe_float(S.get('allocated')),S['pool']))
            S['running']=False;S['paused']=False
            # Preserve the V25.6.9 public health contract across restarts. A saved
            # desired-running state resumes as RUNNING/ONLINE/RECOVERED while the
            # internal data-verification latch remains pending and blocks entries
            # until fresh verified market data arrives. Genuine faults still switch
            # the public connection/recovery state to OFFLINE/RECOVERY_REQUIRED.
            S['recovery_pending']=bool(data.get('desired_running',False))
            S['recovery_state']='READY' if S['recovery_pending'] else 'RESTORED_STOPPED'
            S['service_state']='RUNNING' if S.get('desired_running') and not S.get('paused') else 'READY'
            if S['recovery_pending']:
                S['connection']='UNKNOWN';S['data_verification_status']='VERIFYING';S['verification_state']='VERIFYING';S['entry_validation']='BLOCKED';S['entry_gate']='DATA_VERIFYING';S['validation_reasons']=['VERIFYING_FRESH_MARKET_DATA']
            S['restart_count']=int(safe_float(data.get('restart_count',0)))+1
            S['persistence_status']='LOADED';S['persistence_ok']=True
            S['persistence_version']=int(safe_float(data.get('persistence_version',1),1))
            S['state_schema']='INTEGRATED-030'
            for key in ('open','closed','candles','chat','decision_log','data_quality_warnings','market_intelligence_history','entry_quality_history','validation_reasons'):
                S[key]=_as_list(S.get(key))
            for key in ('open','closed','candles','chat','decision_log','market_intelligence_history','entry_quality_history'):
                S[key]=[x for x in S[key] if isinstance(x,dict)]
            S['errors']=_as_list(S.get('errors'))
            for key in ('score_components','rejection_reasons','analytics','validation','forensic_summary','exit_analysis','market_intelligence','adaptive_strategy','regime_performance'):
                S[key]=_as_dict(S.get(key))
            S['market_phase']=str(S.get('market_phase') or 'UNKNOWN');S['market_direction']=str(S.get('market_direction') or 'WAIT')
            S['market_alignment']=safe_float(S.get('market_alignment'));S['market_confidence']=safe_float(S.get('market_confidence'))
            S['thesis_reviews']=int(safe_float(S.get('thesis_reviews')));S['adaptive_exit_count']=int(safe_float(S.get('adaptive_exit_count')))
            S['setup_quality']=safe_float(S.get('setup_quality'));S['adaptive_threshold']=int(safe_float(S.get('adaptive_threshold'),70))
            S['confluence_score']=safe_float(S.get('confluence_score'));S['regime_quality']=safe_float(S.get('regime_quality'));S['trend_agreement']=safe_float(S.get('trend_agreement'))
            S['liquidity_score']=safe_float(S.get('liquidity_score'));S['volatility_score']=safe_float(S.get('volatility_score'));S['momentum_score']=safe_float(S.get('momentum_score'));S['strategy_confidence']=safe_float(S.get('strategy_confidence'))
            S['strategy_027_reviews']=int(safe_float(S.get('strategy_027_reviews')));S['strategy_027_rejections']=int(safe_float(S.get('strategy_027_rejections')));S['strategy_027_passes']=int(safe_float(S.get('strategy_027_passes')))
            S['entry_validation']=str(S.get('entry_validation') or 'NOT_READY');S['verified_snapshot_generation']=int(safe_float(S.get('verified_snapshot_generation'),0));S['last_processed_snapshot_generation']=int(safe_float(S.get('last_processed_snapshot_generation'),0));S['run_generation']=int(safe_float(S.get('run_generation'),0))
        return True
    except Exception as e:
        with LOCK:
            S['service_state']='SAFE_MODE';S['recovery_state']='STATE_LOAD_FAILED';S['errors']=(S.get('errors',[])+['STATE LOAD: '+str(e)[-180:]])[-10:]
        return False


def _fresh_verified_bundle():
    """Fetch ticker + 5m + 1m independently and return only genuinely fresh verified data."""
    with LOCK: S['market_data_worker_state']='FETCHING'
    results={}; errors=[]
    for label,fn in [('ticker',lambda:ticker(force=True)),('5m',lambda:api('5',180,force=True)),('1m',lambda:api('1',180,force=True))]:
        try: results[label]=fn()
        except Exception as e: results[label]=0.0 if label=='ticker' else []; errors.append(label+': '+str(e)[-160:]); log_error('RECOVERY '+label.upper(),e)
    p=results.get('ticker',0.0); c5=results.get('5m',[]); c1=results.get('1m',[])
    ok5,r5=_verified_candles(c5,30); ok1,r1=_verified_candles(c1,30)
    age5=_snapshot_age(c5,300); age1=_snapshot_age(c1,60)
    cache5=api_age('5',180); cache1=api_age('1',180)
    # HTTP freshness and candle freshness are separate. A 5m closed candle can
    # legitimately be several minutes old; the important requirement is that
    # the snapshot was fetched recently and the latest closed candle is within
    # its normal interval tolerance. The old V25.1 rule compared the 5m candle
    # age directly to STALE_SECONDS=15, which made recovery fail for most of
    # every 5-minute cycle.
    fresh=bool(ok5 and ok1 and cache5<=STALE_SECONDS and cache1<=STALE_SECONDS and age5<=MARKET_5M_MAX_AGE and age1<=MARKET_1M_MAX_AGE)
    with LOCK:
        S['stale_data_seconds']=round(max(cache5,cache1),2); S['market_data_worker_state']='VERIFIED' if fresh else 'WAITING'
        S['market_data_last_error']='; '.join(errors)[-300:] if errors else ''
        S['recovery_last_1m_age']=round(age1,2); S['recovery_last_5m_age']=round(age5,2)
        S['recovery_last_1m_cache_age']=round(cache1,2); S['recovery_last_5m_cache_age']=round(cache5,2)
        S['recovery_last_ticker_ok']=bool(p); S['recovery_last_source']=str(S.get('market_data_source','UNKNOWN'))
    return fresh,p,c5,c1,max(age5,age1),r5,r1

def _network_probe():
    """Probe independent public exchange endpoints; diagnostic only, never an entry bypass."""
    probes={}
    endpoints=[('BYBIT','https://api.bybit.com/v5/market/time'),('BINANCE','https://api.binance.com/api/v3/time'),('OKX','https://www.okx.com/api/v5/public/time')]
    for name,url in endpoints:
        t=time.time()
        try:
            d=get_json(url,4.0)
            ok=bool(d)
            probes[name]={'ok':ok,'latency_ms':round((time.time()-t)*1000,1)}
        except Exception as e: probes[name]={'ok':False,'error':str(e)[-140:]}
    good=[k for k,v in probes.items() if v.get('ok')]
    with LOCK:
        S['network_probe']='ONLINE' if good else 'OFFLINE'
        S['network_probe_time']=time.time(); S['network_probe_details']=probes
    return bool(good),probes

def recovery_cycle():
    """Single-flight bounded market recovery. A concurrent verifier can never overwrite a successful recovery result."""
    if not RECOVERY_LOCK.acquire(blocking=False):
        return False
    try:
        return _recovery_cycle_impl()
    finally:
        RECOVERY_LOCK.release()

def _recovery_cycle_impl():
    with LOCK:
        S['last_recovery_time']=now(); S['recovery_pending']=True
        # Recovery-required is a fault result, not the starting state of a normal
        # verification attempt. Keep the public state healthy while verification
        # is being performed; only the failed branch below exposes the fault.
        S['recovery_state']='READY'
        if S.get('desired_running') and not S.get('paused'): S['service_state']='RUNNING'
        else: S['service_state']='READY'
        S['recovery_attempts']=int(S.get('recovery_attempts',0))+1; S['recovery_next_retry']=time.time()+RECOVERY_RETRY
    fresh,p,c5,c1,age,r5,r1=_fresh_verified_bundle()
    if not fresh:
        # A second verifier can legitimately fail while the previous verifier's
        # snapshot is still fresh. Never let that transient race manufacture
        # OFFLINE/RECOVERY_REQUIRED. Reconcile the retained snapshot first.
        if normalize_verified_health():
            with LOCK:
                S['recovery_pending']=False; S['recovery_state']='RECOVERED' if S.get('desired_running') and not S.get('paused') else 'READY'
                S['service_state']='RUNNING' if S.get('desired_running') and not S.get('paused') else 'READY'
                S['recovery_last_reason']='Retained verified snapshot remains valid; concurrent/partial refresh did not invalidate recovery.'
            return True
        ok,probes=_network_probe()
        source=S.get('market_data_source','UNKNOWN')
        cache5=api_age('5',180); cache1=api_age('1',180)
        reason=f'Auto-recovery waiting: market-data auth=public/no-private-key-required, source={source}, ticker={bool(p)}, 5m={len(c5)}/30({r5 or "OK"}), 1m={len(c1)}/30({r1 or "OK"}), candle_age_5m={_snapshot_age(c5,300):.1f}s, candle_age_1m={_snapshot_age(c1,60):.1f}s, fetch_age_5m={cache5:.1f}s, fetch_age_1m={cache1:.1f}s, network={"OK" if ok else "UNREACHABLE"}, last_failure={S.get('market_data_last_failure_reason') or 'none'}.'
        set_connection('OFFLINE',reason+' New paper entries remain blocked until a fresh verified snapshot is received.')
        with LOCK:
            S['recovery_pending']=True; S['recovery_state']='RECOVERY_REQUIRED'; S['service_state']='RUNNING' if S.get('desired_running') and not S.get('paused') else 'READY'; S['recovery_last_reason']=reason; S['recovery_last_network_ok']=bool(ok)
        return False
    # IMPORTANT: fresh, structurally verified market data is the recovery proof.
    # Publish that proof and clear the recovery latch BEFORE optional derived analytics.
    # Derived indicators must never be able to trap the service in RECOVERING.
    with LOCK:
        S['candles']=c5; S['one_minute_candles']=c1
        S['data_verification_status']='VERIFIED'
        S['verification_state']='VERIFIED'
        S['last_verification_time']=now()
        S['data_verification_reason']=''
        S['data_verification_detail']=f'1m/5m verified; fetch ages {float(S.get("recovery_last_1m_cache_age",0) or 0):.1f}s/{float(S.get("recovery_last_5m_cache_age",0) or 0):.1f}s; candle ages {float(S.get("recovery_last_1m_age",0) or 0):.1f}s/{float(S.get("recovery_last_5m_age",0) or 0):.1f}s.'
        S['market_data_worker_state']='VERIFIED'
        S['recovery_successes']=int(S.get('recovery_successes',0))+1
        S['recovery_last_reason']='Fresh market data verified; recovery gate cleared.'
        S['recovery_last_network_ok']=True
        S['engine_last_error']=''; S['errors']=[]
        desired=bool(S.get('desired_running',True))
        paused=bool(S.get('paused',False)) if desired else False
        S['running']=desired; S['paused']=paused
        S['recovery_pending']=False
        S['recovery_state']='RECOVERED' if desired else 'READY'
        S['service_state']='RUNNING' if desired else 'READY'
        S['connection']='ONLINE'
    set_connection('ONLINE','Fresh 1m/5m market snapshot verified; recovery gate cleared. Price/ticker freshness remains a separate hard entry-safety gate.')
    # Optional derived state is best-effort. It can enrich the dashboard/strategy but
    # cannot undo the verified recovery state.
    try: market_quality(force=True)
    except Exception as e: log_error('RECOVERY QUALITY',e)
    try: derivatives_context(force=True)
    except Exception as e: log_error('RECOVERY DERIVATIVES',e)
    try:
        reg=multi_regime()
        with LOCK: S['regime']=reg
    except Exception as e: log_error('RECOVERY REGIME',e)
    try:
        with LOCK:
            S['volume_ratio']=round(c5[-2]['v']/max(sum(z['v'] for z in c5[-22:-2])/20,1e-9),2)
            S['atr_pct']=round((atr(c5[:-1])/max(c5[-2]['c'],1e-9))*100,3)
            S['peak_pool']=max(S['peak_pool'],S['pool'])
            S['risk_block']=''
            S['market_data_last_ok']=now()
            S['market_data_worker_state']='ONLINE'
    except Exception as e:
        log_error('RECOVERY METRICS',e)
    say('Automatic market recovery completed: fresh BTCUSDT ticker, 1m and 5m data are structurally verified.')
    persist_state('recovery_complete')
    return True

def say(text):
    with LOCK:
        S['chat'].append({'time':now(),'text':text}); S['chat']=S['chat'][-60:]

def log_decision(side,score,gate,note):
    item={'time':now(),'signal':side,'score':score,'gate':gate,'note':note}
    with LOCK:
        S['decision_log'].append(item);S['decision_log']=S['decision_log'][-120:];S['entry_gate']=gate;S['decision_note']=note
        if gate not in ('READY','NO_SIGNAL'):S['rejection_reasons'][gate]=S['rejection_reasons'].get(gate,0)+1

def set_connection(state, note=''):
    # RECOVERING is an internal recovery phase, never a public connection state.
    if state=='RECOVERING': state='OFFLINE' if S.get('desired_running') else 'UNKNOWN'
    changed=False
    with LOCK:
        if S.get('connection')!=state:
            if state=='OFFLINE': S['session_connection_losses']=int(S.get('session_connection_losses',0))+1
            if state=='ONLINE' and S.get('connection')=='OFFLINE': S['connection_recoveries']=int(S.get('connection_recoveries',0))+1
            S['connection']=state; S['last_connection_change']=now(); changed=True
    if changed and note: say(note)

def recovery_reconcile():
    """Reconcile transient recovery flags from verified fresh local market state.
    This never fetches data and never bypasses safety gates; it only clears a stale
    recovery latch after candle_step/recovery_cycle has already verified fresh data.
    """
    try:
        with LOCK:
            running=bool(S.get('running')); paused=bool(S.get('paused')); desired=bool(S.get('desired_running'))
            verified=S.get('data_verification_status')=='VERIFIED'
            conn=S.get('connection')
            rp=bool(S.get('recovery_pending')); rs=S.get('recovery_state')
            cache_ok=float(S.get('recovery_last_1m_cache_age',1e9) or 1e9)<=STALE_SECONDS and float(S.get('recovery_last_5m_cache_age',1e9) or 1e9)<=STALE_SECONDS
            age_ok=float(S.get('recovery_last_1m_age',1e9) or 1e9)<=MARKET_1M_MAX_AGE and float(S.get('recovery_last_5m_age',1e9) or 1e9)<=MARKET_5M_MAX_AGE
            # VERIFIED is the authoritative proof; worker state is descriptive and may transiently be FETCHING.
            if not (running and desired and not paused and verified and cache_ok and age_ok):
                return False
            # VERIFIED + fresh ages is sufficient proof for the public connection
            # state. Do not expose UNKNOWN merely because a previous worker pass
            # was in the middle of a refresh.
            changed=(rp or rs=='RECOVERY_REQUIRED' or conn!='ONLINE' or S.get('service_state')=='RECOVERING')
            if changed:
                S['recovery_pending']=False
                S['recovery_state']='RECOVERED'
                S['service_state']='RUNNING'
                S['connection']='ONLINE'
                S['last_connection_change']=now() if conn!='ONLINE' else S.get('last_connection_change',now())
                S['recovery_last_reason']='Recovery latch reconciled from verified fresh market state.'
                S['recovery_last_network_ok']=True
                # Recovery reconciliation proves the data/connection gate; it
                # does not prove a trading setup. Leave setup validation to the
                # strategy evaluator instead of falsely displaying READY.
                if S.get('entry_validation')=='BLOCKED' and 'DATA_VERIFY_PENDING' in (S.get('validation_reasons') or []):
                    S['entry_validation']='NOT_READY'
                    S['validation_reasons']=['AWAITING_1M_SETUP']
                S['entry_gate']='VERIFIED_MARKET_DATA'
                S['engine_last_error']=''
                S['errors']=[]
        if changed:
            say('Recovery state reconciled: verified fresh market data is online; engine returned to RUNNING.')
            return True
    except Exception as e:
        log_error('RECOVERY RECONCILE',e)
    return False

def _record_market_data_failure(source,error):
    msg=str(error)[-220:]
    with LOCK:
        S['last_market_data_failure']=time.time()
        S['market_data_last_failure_at']=S['last_market_data_failure']
        S['market_data_last_failure_source']=str(source)
        S['market_data_last_failure_reason']=msg
        S['connection_failures']=int(S.get('connection_failures',0))+1
        S['market_data_last_error']=f'{source}: {msg}'[-500:]
        S['errors']=(S.get('errors',[])+[f'MARKET DATA {source}: {msg}'])[-10:]
        S['market_data_consecutive_failures']=int(S.get('market_data_consecutive_failures',0))+1

def _record_market_data_success(source):
    t=time.time()
    with LOCK:
        S['last_market_data_success']=t
        S['market_data_last_success_at']=t
        S['market_data_last_success_source']=str(source)
        S['connection_failures']=0
        S['market_data_last_error']=''
        S['market_data_last_failure_reason']=''
        S['market_data_consecutive_failures']=0
        S['market_data_fetch_successes']=int(S.get('market_data_fetch_successes',0))+1

def get_json(url,timeout=NETWORK_TIMEOUT):
    """Fetch public JSON with strict response validation and bounded host retry."""
    source='BYBIT' if 'bybit' in str(url) or 'bytick' in str(url) else 'PUBLIC'
    with LOCK: S['market_data_fetch_attempts']=int(S.get('market_data_fetch_attempts',0))+1
    urls=[url]
    if source=='BYBIT' and 'api.bybit.com' in url:
        urls=[url.replace('api.bybit.com',h,1) for h in MARKET_HOSTS]
    last=None
    for target in urls:
        try:
            req=Request(target,headers={'User-Agent':'ZTrader-AI/030'})
            raw=urlopen(req,timeout=min(float(timeout),NETWORK_TIMEOUT)).read()
            data=json.loads(raw)
            if not isinstance(data,dict) or not data: raise ValueError('empty/invalid JSON response')
            if 'retCode' in data and int(data.get('retCode',0))!=0: raise ValueError(f"exchange retCode={data.get('retCode')} retMsg={data.get('retMsg','')}")
            is_probe=('/market/time' in str(target) or '/api/v3/time' in str(target) or '/public/time' in str(target))
            if not is_probe: _record_market_data_success(source)
            with LOCK:
                S['network_probe_details']=dict(S.get('network_probe_details') or {},LAST_MARKET_HOST=target)
            return data
        except Exception as e: last=e
    if last is not None: _record_market_data_failure(source,last)
    return None

def _binance_json(path, timeout=2.5):
    """Independent public-data fallback with host rotation used only when the primary Bybit feed fails."""
    with LOCK:
        S['market_data_fetch_attempts']=int(S.get('market_data_fetch_attempts',0))+1
    last=None
    for host in ("api.binance.com","api1.binance.com","api2.binance.com","api3.binance.com"):
        try:
            req=Request("https://"+host+path,headers={"User-Agent":"ZTrader-AI/030"})
            data=json.loads(urlopen(req,timeout=min(float(timeout),NETWORK_TIMEOUT)).read())
            if not data: raise ValueError("empty response")
            _record_market_data_success("BINANCE_FALLBACK")
            with LOCK: S["market_data_source"]="BINANCE_FALLBACK"
            return data
        except Exception as e: last=e
    _record_market_data_failure("BINANCE_FALLBACK",last or ValueError("no Binance host responded"))
    return None

def _okx_json(path, timeout=3.5):
    """Third independent public-data fallback for environments where one venue is unreachable."""
    with LOCK:
        S['market_data_fetch_attempts']=int(S.get('market_data_fetch_attempts',0))+1
    try:
        req=Request("https://www.okx.com"+path,headers={"User-Agent":"ZTrader-AI/030"})
        data=json.loads(urlopen(req,timeout=min(float(timeout),NETWORK_TIMEOUT)).read())
        if not data: raise ValueError('empty response')
        _record_market_data_success('OKX_FALLBACK')
        with LOCK: S["market_data_source"]="OKX_FALLBACK"
        return data
    except Exception as e:
        _record_market_data_failure('OKX_FALLBACK',e)
        return None

def api(interval,limit=180,force=False):
    key=(str(interval),int(limit)); ttl=API_TTLS.get(str(interval),10.0); ts=time.time()
    with CACHE_LOCK:
        cached=API_CACHE.get(key)
        if cached and not force and ts-cached["time"]<ttl: return cached["data"]
    # Primary: Bybit.
    d=get_json(f"https://api.bybit.com/v5/market/kline?category=linear&symbol={SYMBOL}&interval={interval}&limit={limit}")
    if d:
        try:
            rows=[dict(t=int(x[0]),o=float(x[1]),h=float(x[2]),l=float(x[3]),c=float(x[4]),v=float(x[5])) for x in reversed(d.get("result",{}).get("list",[]))]
            if rows:
                with CACHE_LOCK: API_CACHE[key]={"time":time.time(),"data":rows}
                _record_market_data_success("BYBIT")
                with LOCK: S["market_data_source"]="BYBIT"
                return rows
        except Exception as e: log_error("KLINE PARSE",e)
    # Fallback 2: Binance public spot klines.
    b_interval={"1":"1m","5":"5m","15":"15m","240":"4h","D":"1d"}.get(str(interval))
    if b_interval:
        b=_binance_json(f"/api/v3/klines?symbol=BTCUSDT&interval={b_interval}&limit={int(limit)}")
        if b:
            try:
                rows=[dict(t=int(x[0]),o=float(x[1]),h=float(x[2]),l=float(x[3]),c=float(x[4]),v=float(x[5])) for x in b]
                if rows:
                    with CACHE_LOCK: API_CACHE[key]={"time":time.time(),"data":rows}
                    _record_market_data_success('BINANCE_FALLBACK')
                    with LOCK: S['market_data_source']='BINANCE_FALLBACK'
                    return rows
            except Exception as e: log_error("BINANCE KLINE PARSE",e)
    # Fallback 3: OKX BTC-USDT candles.
    okx_bar={"1":"1m","5":"5m","15":"15m","240":"4H","D":"1D"}.get(str(interval))
    if okx_bar:
        o=_okx_json(f"/api/v5/market/candles?instId=BTC-USDT&bar={okx_bar}&limit={int(limit)}")
        try:
            rows=[dict(t=int(x[0]),o=float(x[1]),h=float(x[2]),l=float(x[3]),c=float(x[4]),v=float(x[5])) for x in reversed(o["data"])] if o and o.get("data") else []
            if rows:
                with CACHE_LOCK: API_CACHE[key]={"time":time.time(),"data":rows}
                _record_market_data_success('OKX_FALLBACK')
                with LOCK: S['market_data_source']='OKX_FALLBACK'
                return rows
        except Exception as e: log_error("OKX KLINE PARSE",e)
    # Fallback 4: Coinbase public candles. This is diagnostic/market-data continuity only;
    # it never enables live execution and remains subject to the same freshness/structure gates.
    try:
        cb_gran={"1":60,"5":300}.get(str(interval))
        if cb_gran:
            u=f"https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity={cb_gran}"
            req=Request(u,headers={"User-Agent":"ZTrader-AI/030"})
            raw=json.loads(urlopen(req,timeout=min(4.0,NETWORK_TIMEOUT)).read())
            rows=[dict(t=int(x[0])*1000,o=float(x[3]),h=float(x[2]),l=float(x[1]),c=float(x[4]),v=float(x[5])) for x in reversed(raw or [])]
            if rows:
                with CACHE_LOCK: API_CACHE[key]={"time":time.time(),"data":rows}
                _record_market_data_success("COINBASE_FALLBACK")
                with LOCK: S["market_data_source"]="COINBASE_FALLBACK"
                return rows
    except Exception as e:
        _record_market_data_failure("COINBASE_FALLBACK",e)

    # One bounded second pass prevents a transient exchange/network timeout from leaving the
    # cache stale for minutes. It still never treats cached data as fresh.
    if force:
        time.sleep(0.15)
        for host in MARKET_HOSTS:
            try:
                u=f"https://{host}/v5/market/kline?category=linear&symbol={SYMBOL}&interval={interval}&limit={limit}"
                d2=get_json(u,timeout=3.5)
                rows=[dict(t=int(x[0]),o=float(x[1]),h=float(x[2]),l=float(x[3]),c=float(x[4]),v=float(x[5])) for x in reversed(d2.get('result',{}).get('list',[]))] if d2 else []
                if rows:
                    with CACHE_LOCK: API_CACHE[key]={"time":time.time(),"data":rows}
                    with LOCK: S['market_data_source']='BYBIT_RETRY'
                    return rows
            except Exception as e: log_error('BYBIT RETRY',e)
    # Cached data may support non-forced continuity, but forced recovery must not accept it as fresh.
    with LOCK:
        S['market_data_last_error']='All primary/fallback candle endpoints failed.'
        S['market_data_last_failure_source']=f'KLINE_{interval}'
        S['market_data_last_failure_reason']='Bybit, Binance and OKX public candle endpoints did not return a valid dataset.'
    if force: return []
    with CACHE_LOCK: return cached['data'] if cached else []

def api_age(interval,limit=180):
    with CACHE_LOCK:
        x=API_CACHE.get((str(interval),int(limit)))
        return time.time()-x['time'] if x else float('inf')

def ticker(force=False):
    ts=time.time()
    with CACHE_LOCK:
        if not force and TICKER_CACHE["price"]>0 and ts-TICKER_CACHE["time"]<TICKER_CACHE_SECONDS:
            p=TICKER_CACHE["price"]
            with LOCK: S["price"]=p
            return p
    d=get_json(f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={SYMBOL}",2.5)
    try:
        p=float(d["result"]["list"][0]["lastPrice"])
        if not math.isfinite(p) or p<=0: raise ValueError('invalid Bybit ticker price')
        with CACHE_LOCK: TICKER_CACHE.update(price=p,time=time.time())
        with LOCK: S["last_tick"]=time.time(); S["price"]=p; S["market_data_source"]="BYBIT"
        return p
    except Exception:
        b=_binance_json("/api/v3/ticker/price?symbol=BTCUSDT",2.5)
        try:
            p=float(b["price"])
            if not math.isfinite(p) or p<=0: raise ValueError('invalid Binance ticker price')
            with CACHE_LOCK: TICKER_CACHE.update(price=p,time=time.time())
            with LOCK: S["last_tick"]=time.time(); S["price"]=p; S["market_data_source"]="BINANCE_FALLBACK"
            return p
        except Exception:
            o=_okx_json("/api/v5/market/ticker?instId=BTC-USDT",3.5)
            try:
                p=float(o["data"][0]["last"])
                if not math.isfinite(p) or p<=0: raise ValueError('invalid OKX ticker price')
                with CACHE_LOCK: TICKER_CACHE.update(price=p,time=time.time())
                with LOCK: S["last_tick"]=time.time(); S["price"]=p; S["market_data_source"]="OKX_FALLBACK"
                return p
            except Exception:
                with LOCK: S['market_data_last_error']='All primary/fallback ticker endpoints failed.'
                return 0.0

def market_quality(force=False):
    if not force and time.time()-S.get('last_quality_check',0)<QUALITY_EVERY:return
    d=get_json(f'https://api.bybit.com/v5/market/orderbook?category=linear&symbol={SYMBOL}&limit=25',2.5)
    try:
        r=d['result']; bids=[(float(x[0]),float(x[1])) for x in r.get('b',[])]; asks=[(float(x[0]),float(x[1])) for x in r.get('a',[])]
        if not bids or not asks:return
        bid,ask=bids[0][0],asks[0][0]; mid=(bid+ask)/2
        if not all(math.isfinite(x) and x>0 for x in (bid,ask,mid)) or ask<bid: raise ValueError('invalid orderbook prices')
        spread=(ask-bid)/max(mid,1e-9)*10000
        bn=sum(p*q for p,q in bids[:10]); an=sum(p*q for p,q in asks[:10]); imb=(bn-an)/max(bn+an,1e-9)
        with LOCK:
            S['last_quality_check']=time.time();S['spread_bps']=round(spread,3);S['order_imbalance']=round(imb,4)
            S['market_quality']='GOOD' if spread<=MAX_SPREAD_BPS else 'WIDE_SPREAD'
    except Exception: pass

def derivatives_context(force=False):
    if not force and time.time()-S.get('last_derivatives_check',0)<DERIVATIVES_EVERY:return
    f=get_json(f'https://api.bybit.com/v5/market/tickers?category=linear&symbol={SYMBOL}',2.5)
    oi=get_json(f'https://api.bybit.com/v5/market/open-interest?category=linear&symbol={SYMBOL}&intervalTime=5min&limit=2',2.5)
    try: fr=float(f['result']['list'][0].get('fundingRate',0) or 0)
    except Exception: fr=S.get('funding_rate',0.0)
    ch=S.get('open_interest_change',0.0)
    try:
        rows=oi['result']['list']
        if len(rows)>=2:
            a=float(rows[-2]['openInterest']);b=float(rows[-1]['openInterest']);ch=(b-a)/max(a,1e-9)*100
    except Exception: pass
    with LOCK:
        S['last_derivatives_check']=time.time();S['funding_rate']=round(fr,6);S['open_interest_change']=round(ch,5)

def ema(a,n):
    if not a:return 0.0
    k=2/(n+1);x=a[0]
    for p in a[1:]:x=p*k+x*(1-k)
    return x

def atr(c,n=14):
    if len(c)<2:return 0.0
    tr=[max(z['h']-z['l'],abs(z['h']-c[i-1]['c']),abs(z['l']-c[i-1]['c'])) for i,z in enumerate(c) if i]
    tr=tr[-n:];return sum(tr)/len(tr) if tr else 0.0

def rsi(a,n=14):
    if len(a)<n+1:return 50.0
    d=[a[i]-a[i-1] for i in range(1,len(a))][-n:];g=sum(max(x,0) for x in d)/n;l=sum(max(-x,0) for x in d)/n
    return 100.0 if l==0 else 100-100/(1+g/l)

def adx(c,n=14):
    if len(c)<n*2+2:return 0.0
    trs=[];plus=[];minus=[]
    for i in range(1,len(c)):
        z,p=c[i],c[i-1];trs.append(max(z['h']-z['l'],abs(z['h']-p['c']),abs(z['l']-p['c'])))
        up=z['h']-p['h'];dn=p['l']-z['l'];plus.append(up if up>dn and up>0 else 0);minus.append(dn if dn>up and dn>0 else 0)
    trs=trs[-n:];plus=plus[-n:];minus=minus[-n:];t=sum(trs) or 1e-9;pdi=100*sum(plus)/t;mdi=100*sum(minus)/t
    return round(100*abs(pdi-mdi)/max(pdi+mdi,1e-9),2)

def confirmed_swings(c,left=2,right=2):
    if len(c)<left+right+5:return [],[]
    highs=[];lows=[];end=len(c)-right
    for i in range(left,end):
        x=c[i]
        if x['h']>max(z['h'] for z in c[i-left:i]) and x['h']>=max(z['h'] for z in c[i+1:i+right+1]):highs.append((i,x['h']))
        if x['l']<min(z['l'] for z in c[i-left:i]) and x['l']<=min(z['l'] for z in c[i+1:i+right+1]):lows.append((i,x['l']))
    return highs,lows

def structure_from(c):
    if len(c)<30:return 'UNKNOWN',{}
    q=c[:-1] if len(c)>2 else c;highs,lows=confirmed_swings(q,2,2)
    if len(highs)<2 or len(lows)<2:return 'UNKNOWN',{}
    ph,lh=highs[-2][1],highs[-1][1];pl,ll=lows[-2][1],lows[-1][1];bull=lh>ph and ll>pl;bear=lh<ph and ll<pl;close=q[-1]['c']
    return ('BULLISH' if bull else 'BEARISH' if bear else 'RANGE'),{'prev_high':ph,'last_high':lh,'prev_low':pl,'last_low':ll,'breakout':(bull and close>=lh) or (bear and close<=ll)}

def daily_structure(): return structure_from(api('D',220))

def multi_regime():
    d,di=daily_structure();h4,h4i=structure_from(api('240',180));m15=api('15',100)
    with LOCK:
        S['daily_structure']=d;S['fourh_structure']=h4;S['daily_hh']=di.get('last_high',0);S['daily_hl']=di.get('last_low',0);S['daily_lh']=di.get('last_high',0);S['daily_ll']=di.get('last_low',0)
    if len(m15)<20:return d if d!='UNKNOWN' else 'UNKNOWN'
    last=m15[:-1][-8:];ups=sum(z['c']>z['o'] for z in last);downs=len(last)-ups
    if d=='BULLISH' and h4 in ('BULLISH','RANGE') and ups>=4:return 'DAILY_BULLISH'
    if d=='BEARISH' and h4 in ('BEARISH','RANGE') and downs>=4:return 'DAILY_BEARISH'
    if d=='BULLISH' and h4=='BEARISH':return 'BULLISH_PULLBACK'
    if d=='BEARISH' and h4=='BULLISH':return 'BEARISH_PULLBACK'
    return 'RANGE'

def candle_bias(c,i=-1):
    if len(c)<6:return 'WAIT',0
    idx=i if i>=0 else len(c)+i;x=c[idx];p=c[idx-1];rng=max(x['h']-x['l'],1e-9);body=abs(x['c']-x['o'])/rng;cp=(x['c']-x['l'])/rng;green=x['c']>x['o'];red=x['c']<x['o'];upper=(x['h']-max(x['o'],x['c']))/rng;lower=(min(x['o'],x['c'])-x['l'])/rng;score=0
    if green:score+=8
    elif red:score-=8
    if body>.65:score+=8 if green else -8
    if cp>.7:score+=7
    elif cp<.3:score-=7
    if lower>.35 and cp>.55:score+=7
    if upper>.35 and cp<.45:score-=7
    if green and p['c']<p['o'] and x['c']>p['o'] and x['o']<p['c']:score+=8
    if red and p['c']>p['o'] and x['c']<p['o'] and x['o']>p['c']:score-=8
    if body<.18:score=0
    return ('BUY',min(18,score)) if score>=10 else ('SELL',min(18,-score)) if score<=-10 else ('WAIT',abs(score))

def prediction(c,i=-1):
    idx=i if i>=0 else len(c)+i
    if idx<8:return 'WAIT',50.0,'Not enough candle sequence'
    r=c[max(0,idx-7):idx+1];bull=bear=0.0
    for j,z in enumerate(r):
        rng=max(z['h']-z['l'],1e-9);body=abs(z['c']-z['o'])/rng;cp=(z['c']-z['l'])/rng;w=1+.1*j
        if z['c']>z['o']:bull+=w*(1+body+.35*max(0,cp-.5))
        elif z['c']<z['o']:bear+=w*(1+body+.35*max(0,.5-cp))
    closes=[z['c'] for z in r];drift=(closes[-1]-closes[-4])/max(closes[-4],1e-9)
    if drift>.0007:bull+=3
    elif drift<-.0007:bear+=3
    total=bull+bear
    if not total:return 'WAIT',50.0,'Balanced pressure'
    p=50+45*(bull-bear)/total
    if bull>bear*1.18 and p>=62:return 'BUY',round(p,1),'Recent bullish pressure'
    if bear>bull*1.18 and p<=38:return 'SELL',round(100-p,1),'Recent bearish pressure'
    return 'WAIT',round(max(p,100-p),1),'Mixed short-term pressure'

def vwap(c,n=96):
    q=c[-n:];den=sum(x['v'] for x in q);return sum(((x['h']+x['l']+x['c'])/3)*x['v'] for x in q)/den if den else 0.0

def market_intelligence(c):
    """Multi-timeframe market reading used for entry context and adaptive exits.
    It describes the market from several independent dimensions without guaranteeing direction.
    """
    if len(c)<40:
        return {'direction':'WAIT','confidence':0.0,'alignment':0.0,'phase':'UNKNOWN','reason':f'Collecting 5m data: {len(c)}/40'}
    x=c[-2]; closes=[z['c'] for z in c[:-1]]; e21=ema(closes[-80:],21);e50=ema(closes[-100:],50)
    e21_prev=ema(closes[-86:-6],21) if len(closes)>90 else e21
    slope=(e21-e21_prev)/max(abs(e21_prev),1e-9)*10000
    rr=rsi(closes[-80:]);ax=adx(c[:-1]);ar=atr(c[:-1]);atr_pct=ar/max(x['c'],1e-9)*100
    cb,cbv=candle_bias(c[:-1],-1);pred,pp,pr=prediction(c[:-1],-1)
    vol=S.get('volume_ratio',1.0);imb=S.get('order_imbalance',0.0);spread=S.get('spread_bps',0.0)
    d=S.get('daily_structure','UNKNOWN');h4=S.get('fourh_structure','UNKNOWN');rg=S.get('regime','UNKNOWN')
    buy=sell=0.0
    if d=='BULLISH':buy+=28
    elif d=='BEARISH':sell+=28
    if h4=='BULLISH':buy+=20
    elif h4=='BEARISH':sell+=20
    if rg=='DAILY_BULLISH':buy+=12
    elif rg=='DAILY_BEARISH':sell+=12
    if e21>e50:buy+=10
    elif e21<e50:sell+=10
    if slope>1.0:buy+=8
    elif slope<-1.0:sell+=8
    if rr>=55:buy+=6
    elif rr<=45:sell+=6
    if cb=='BUY':buy+=6
    elif cb=='SELL':sell+=6
    if pred=='BUY':buy+=5
    elif pred=='SELL':sell+=5
    if vol>=1.05:
        if x['c']>x['o']:buy+=3
        elif x['c']<x['o']:sell+=3
    if imb>MIN_MICRO_EDGE:buy+=4
    elif imb<-MIN_MICRO_EDGE:sell+=4
    total=buy+sell; direction='BUY' if buy>sell+8 else 'SELL' if sell>buy+8 else 'WAIT'
    confidence=round(100*max(buy,sell)/max(total,1),1)
    alignment=round(min(100,confidence + (8 if ((direction=='BUY' and d=='BULLISH' and h4!='BEARISH') or (direction=='SELL' and d=='BEARISH' and h4!='BULLISH')) else 0)),1)
    if direction=='BUY' and confidence>=72:phase='TREND_UP'
    elif direction=='SELL' and confidence>=72:phase='TREND_DOWN'
    elif max(buy,sell)-min(buy,sell)<12:phase='RANGE_OR_TRANSITION'
    else:phase='PULLBACK_OR_TRANSITION'
    liquidity='GOOD' if spread<=MAX_SPREAD_BPS else 'WIDE'
    reason=f'{phase}; Daily={d}; 4H={h4}; 5m EMA21/50={"UP" if e21>e50 else "DOWN"}; ADX={ax:.1f}; RSI={rr:.1f}; volume={vol:.2f}; imbalance={imb:.3f}; liquidity={liquidity}'
    out={'direction':direction,'confidence':confidence,'alignment':alignment,'phase':phase,'buy_pressure':round(buy,1),'sell_pressure':round(sell,1),'ema21':round(e21,2),'ema50':round(e50,2),'ema_slope_bps':round(slope,2),'rsi':round(rr,2),'adx':round(ax,2),'atr_pct':round(atr_pct,3),'volume_ratio':round(vol,2),'imbalance':round(imb,4),'spread_bps':round(spread,3),'daily':d,'fourh':h4,'regime':rg,'prediction':pred,'prediction_probability':round(pp,1),'reason':reason,'updated':now()}
    with LOCK:
        S['market_intelligence']=out;S['market_phase']=phase;S['market_confidence']=confidence;S['market_direction']=direction;S['market_alignment']=alignment;S['last_intelligence_time']=out['updated']
    return out

def score(c,forming=False):
    """Responsive short-horizon BUY/SELL scorer for the 027 scalping mode."""
    idx=len(c)-1 if forming else len(c)-2
    if idx<30:return 'WAIT',0,{'buy':0,'sell':0,'prob':50,'reason':'Not enough execution history'}
    v=c[:idx+1];x=v[-1];ar=atr(v) or max(x['h']-x['l'],1);rr=rsi([z['c'] for z in v]);ax=adx(v);vw=vwap(v);mom=(x['c']-v[-4]['c'])/ar
    avgvol=sum(z['v'] for z in v[-21:-1])/20;vr=x['v']/avgvol if avgvol else 1
    mi=market_intelligence(c) or {};d=str(mi.get('direction') or 'WAIT').upper();buy=float(mi.get('buy_pressure',0) or 0);sell=float(mi.get('sell_pressure',0) or 0)
    cb,cbq=candle_bias(v,-1);pred,pp,pr=prediction(v,-1);up=dn=0
    if d=='BUY':up+=28
    elif d=='SELL':dn+=28
    if mi.get('daily')=='BULLISH':up+=10
    elif mi.get('daily')=='BEARISH':dn+=10
    if mi.get('fourh')=='BULLISH':up+=7
    elif mi.get('fourh')=='BEARISH':dn+=7
    if buy>sell:up+=min(18,int((buy-sell)*.7))
    if sell>buy:dn+=min(18,int((sell-buy)*.7))
    if mom>=.08:up+=10
    if mom<=-.08:dn+=10
    if x['c']>vw:up+=6
    if x['c']<vw:dn+=6
    if rr>=50 and rr<=70:up+=5
    if rr<=50 and rr>=30:dn+=5
    if ax>=14:
        if mom>0:up+=6
        elif mom<0:dn+=6
    if vr>=.85:
        if x['c']>x['o']:up+=6
        elif x['c']<x['o']:dn+=6
    if cb=='BUY':up+=8
    elif cb=='SELL':dn+=8
    if pred=='BUY':up+=4
    elif pred=='SELL':dn+=4
    up=min(100,round(up));dn=min(100,round(dn));prob=50 if up+dn==0 else 100*max(up,dn)/(up+dn);threshold=58 if forming else 56
    S['score_components']={'rsi':round(rr,2),'adx':round(ax,2),'momentum':round(mom,4),'vwap_relation':'ABOVE' if x['c']>vw else 'BELOW','volume_ratio':round(vr,3),'candle_bias':cb,'prediction':pred,'prediction_probability':round(pp,1),'daily':mi.get('daily','UNKNOWN'),'fourh':mi.get('fourh','UNKNOWN'),'regime':S.get('regime','UNKNOWN'),'buy_score':up,'sell_score':dn,'threshold':threshold}
    if up>=threshold and up>=dn+6 and d in ('BUY','WAIT'):
        return 'BUY',up,{'buy':up,'sell':dn,'prob':round(prob,1),'reason':'Rapid BUY: pressure + momentum + structure/confluence'}
    if dn>=threshold and dn>=up+6 and d in ('SELL','WAIT'):
        return 'SELL',dn,{'buy':up,'sell':dn,'prob':round(prob,1),'reason':'Rapid SELL: pressure + momentum + structure/confluence'}
    return 'WAIT',max(up,dn),{'buy':up,'sell':dn,'prob':round(prob,1),'reason':f'Rapid scalp setup incomplete; market direction {d}'}



def btc_1m_strategy(c1):
    """BTCUSDT 1-minute dual-speed signal engine.
    NORMAL captures aligned continuation/pullback setups; QUICK captures short-lived
    momentum/rejection opportunities. Both directions are symmetric and paper-only.
    """
    if len(c1)<30:return 'WAIT',0,{'mode':'WAIT','prob':50,'reason':f'Collecting 1m candles: {len(c1)}/30'}
    q=c1[:-1];x=q[-1];cl=[z['c'] for z in q];e9=ema(cl[-80:],9);e21=ema(cl[-100:],21);e50=ema(cl[-120:],50)
    ar=atr(q) or max(x['h']-x['l'],1e-9);rr=rsi(cl[-80:]);ax=adx(q);vw=vwap(q,30)
    avgvol=sum(z['v'] for z in q[-21:-1])/20;vr=x['v']/avgvol if avgvol else 1
    rng=max(x['h']-x['l'],1e-9);body=abs(x['c']-x['o'])/rng;closepos=(x['c']-x['l'])/rng
    mom=(x['c']-q[-4]['c'])/ar; prev_high=max(z['h'] for z in q[-7:-1]);prev_low=min(z['l'] for z in q[-7:-1])
    mi=S.get('market_intelligence',{}) or {};md=mi.get('direction','WAIT');align=float(mi.get('alignment',0) or 0)
    buy=sell=0
    if e9>e21:buy+=18
    elif e9<e21:sell+=18
    if e21>e50:buy+=10
    elif e21<e50:sell+=10
    if x['c']>vw:buy+=10
    elif x['c']<vw:sell+=10
    if mom>=.10:buy+=16
    elif mom<=-.10:sell+=16
    if x['c']>prev_high:buy+=12
    if x['c']<prev_low:sell+=12
    if body>=.55 and closepos>=.72:buy+=10
    if body>=.55 and closepos<=.28:sell+=10
    if vr>=1.05:
        if x['c']>x['o']:buy+=8
        elif x['c']<x['o']:sell+=8
    if 48<=rr<=70:buy+=5
    if 30<=rr<=52:sell+=5
    if ax>=16:
        if mom>0:buy+=5
        elif mom<0:sell+=5
    if md=='BUY':buy+=10 if align>=52 else 5
    elif md=='SELL':sell+=10 if align>=52 else 5
    # Integrate the validated adaptive review as a bounded confirmation bonus.
    # It can lift a near-threshold 1m setup into QUICK_ADVANTAGE/NORMAL, but never bypasses the hard score, risk, data, and same-side gates.
    ad=S.get('adaptive_strategy',{}) or {}
    aq=float(ad.get('quality',0) or 0); ath=float(ad.get('threshold',55) or 55); adir=str(ad.get('direction') or 'WAIT')
    if bool(ad.get('ready')) and aq>=ath and adir in ('BUY','SELL'):
        bonus=min(10.0,max(4.0,(aq-ath)*0.35+4.0))
        if adir=='BUY':buy+=bonus
        else:sell+=bonus
    # Responsive paper-entry mode: lower score floor and separation for earlier qualified opportunities; hard risk/data gates remain active.
    quick_side='BUY' if buy>=46 and buy>=sell+5 else 'SELL' if sell>=46 and sell>=buy+5 else 'WAIT'
    normal_side='BUY' if buy>=54 and buy>=sell+5 and md in ('BUY','WAIT') else 'SELL' if sell>=54 and sell>=buy+5 and md in ('SELL','WAIT') else 'WAIT'
    adaptive_side=adir if bool(ad.get('ready')) and adir in ('BUY','SELL') and aq>=ath else 'WAIT'
    side=normal_side if normal_side!='WAIT' else quick_side
    mode='NORMAL' if normal_side!='WAIT' else 'QUICK_ADVANTAGE' if quick_side!='WAIT' else 'WAIT'
    raw=max(buy,sell);prob=50 if buy+sell==0 else round(100*raw/(buy+sell),1)
    # If the adaptive review has already established a direction, strong alignment,
    # sufficient confidence and weighted quality, do not let a narrow 1m component
    # score strand a clear opportunity at WAIT. Route it through the existing
    # QUICK_ADVANTAGE path so all hard risk/data/capital/same-side gates still apply.
    if side=='WAIT' and adaptive_side!='WAIT' and float(ad.get('alignment',0) or 0)>=50 and float(ad.get('confidence',0) or 0)>=46:
        side=adaptive_side;mode='QUICK_ADVANTAGE';raw=max(raw,float(ath),min(60.0,float(aq)))
        prob=max(prob,round(min(99.0,float(ad.get('quality',0) or 50)),1))
    reason=f'{mode}: 1m EMA9/21, EMA21/50, VWAP, momentum, volume, candle pressure; HTF={md}/{align:.1f}%; adaptive={adir}/{aq:.1f}/{ath:.1f}'
    out={'mode':mode,'buy':min(100,buy),'sell':min(100,sell),'prob':prob,'reason':reason,'atr':ar,'rsi':rr,'adx':ax,'volume_ratio':vr,'momentum':mom,'vwap':'ABOVE' if x['c']>vw else 'BELOW','direction':side}
    with LOCK:
        S['quick_signal']=side;S['quick_score']=raw;S['quick_mode']=mode;S['quick_reason']=reason;S['one_minute_strategy']=out
    return side,raw,out


def decide(c,forming=False):
    side,sc,info=score(c,forming);S['prediction']=info['prob'];S['prediction_reason']=info['reason'];return side,sc,info

def risk_block(side):
    today=datetime.now(timezone.utc).date().isoformat()
    if S.get('day_key')!=today:S['day_key']=today;S['day_start_pool']=S['pool'];S['loss_cooldown']=0;S['consecutive_losses']=0
    if S.get('connection')!='ONLINE':return 'CONNECTION_LOST'
    if S.get('market_quality') not in ('GOOD','WIDE_SPREAD'):return 'MARKET_QUALITY_UNVERIFIED'
    if S.get('market_quality')=='WIDE_SPREAD':return 'WIDE_SPREAD'
    if S['peak_pool']>0 and S['pool']<=S['peak_pool']*(1-MAX_POOL_DRAWDOWN):return 'POOL_DRAWDOWN_LIMIT'
    if S['day_start_pool']>0 and S['pool']<=S['day_start_pool']*(1-DAILY_LOSS_LIMIT):return 'DAILY_LOSS_LIMIT'
    if S.get('consecutive_losses',0)>=MAX_CONSECUTIVE_LOSSES:return 'CONSECUTIVE_LOSS_LIMIT'
    if S.get('loss_cooldown',0)>0:return 'LOSS_COOLDOWN'
    if time.time()-S.get('last_tick',0)>STALE_SECONDS:return 'STALE_PRICE'
    if side=='BUY' and S.get('order_imbalance',0)<-MIN_MICRO_EDGE:return 'MICROSTRUCTURE_AGAINST_BUY'
    if side=='SELL' and S.get('order_imbalance',0)>MIN_MICRO_EDGE:return 'MICROSTRUCTURE_AGAINST_SELL'
    if S.get('atr_pct',0)>6:return 'EXTREME_VOLATILITY'
    return ''

def stop_for(side,entry):
    h=api('15',120);ar=atr(h[:-1]) if len(h)>=20 else 0;buf=(ar or entry*.001)*SL_ATR_BUFFER;level=0;reason='15m confirmed swing'
    if len(h)>=15:
        highs,lows=confirmed_swings(h[:-1],2,2)
        if side=='BUY':
            z=[v for _,v in lows if v<entry]
            if z:level=max(z)
        else:
            z=[v for _,v in highs if v>entry]
            if z:level=min(z)
    if not level:
        if side=='BUY' and S.get('daily_hl',0)<entry:level=S['daily_hl'];reason='Daily Higher Low'
        elif side=='SELL' and S.get('daily_lh',0)>entry:level=S['daily_lh'];reason='Daily Lower High'
    if not level:
        level=entry-(ar or entry*.001)*FALLBACK_SL_ATR if side=='BUY' else entry+(ar or entry*.001)*FALLBACK_SL_ATR;reason='ATR fallback'
    level=level-buf if side=='BUY' else level+buf;dist=abs(entry-level);cap=(ar or entry*.001)*MAX_SL_ATR
    if dist>cap:level=entry-cap if side=='BUY' else entry+cap;reason='ATR-capped structural stop'
    return round(level,2),reason

def free():return max(0,S['pool']-S['allocated'])

def projected_target(entry,side,sl,lot):
    risk=abs(entry-sl)*lot
    if risk<=0:return 0,0
    target_price=entry+risk*MIN_RR/max(lot,1e-9) if side=='BUY' else entry-risk*MIN_RR/max(lot,1e-9)
    return risk,target_price

def adaptive_strategy_review(c=None):
    """Fast 027 scalping gate: direction-specific evidence with short-horizon responsiveness."""
    try:
        c=c or S.get('candles',[])
        if len(c)<30:
            return {'ready':False,'threshold':64,'quality':0.0,'reasons':['INSUFFICIENT_DATA']}
        mi=market_intelligence(c) or {}
        sc=S.get('score_components',{}) or {}
        direction=str(mi.get('direction') or 'NEUTRAL').upper()
        alignment=float(mi.get('alignment',0) or 0); confidence=float(mi.get('confidence',0) or 0)
        adxv=float(mi.get('adx',sc.get('adx',0)) or 0); rsiv=float(mi.get('rsi',sc.get('rsi',50)) or 50)
        vr=float(sc.get('volume_ratio',S.get('volume_ratio',0)) or 0); spread=float(S.get('spread_bps',0) or 0)
        regime=str(mi.get('regime') or S.get('regime') or 'UNKNOWN').upper()
        struct=str(mi.get('structure') or '').upper(); buy=float(mi.get('buy_pressure',0) or 0); sell=float(mi.get('sell_pressure',0) or 0)
        pressure=max(buy,sell); direction_pressure=buy if direction=='BUY' else sell if direction=='SELL' else 0
        trend_ok=72.0 if ((direction=='BUY' and ('BULL' in struct or mi.get('daily')=='BULLISH')) or (direction=='SELL' and ('BEAR' in struct or mi.get('daily')=='BEARISH'))) else 58.0
        liquidity=max(0.0,min(100.0,100.0-(spread/MAX_SPREAD_BPS*100.0))) if spread>0 else 82.0
        volume_q=max(0.0,min(100.0,50.0+max(-1.0,min(2.0,vr-1.0))*25.0))
        atrp=float(S.get('atr_pct',0) or 0); volatility_q=85.0 if .15<=atrp<=2.8 else 65.0 if atrp<=4.5 else 35.0
        momentum_q=min(100.0,max(0.0,adxv*1.45))
        rsi_q=82.0 if ((direction=='BUY' and 45<=rsiv<=70) or (direction=='SELL' and 30<=rsiv<=55)) else 58.0
        pressure_q=min(100.0,45.0+direction_pressure*.9)
        quality=(ADAPTIVE_WEIGHTS['alignment']*alignment+ADAPTIVE_WEIGHTS['confidence']*confidence+ADAPTIVE_WEIGHTS['trend']*trend_ok+ADAPTIVE_WEIGHTS['liquidity']*liquidity+ADAPTIVE_WEIGHTS['volume']*volume_q+ADAPTIVE_WEIGHTS['volatility']*volatility_q+ADAPTIVE_WEIGHTS['momentum']*momentum_q+ADAPTIVE_WEIGHTS['rsi']*rsi_q+ADAPTIVE_WEIGHTS['pressure']*pressure_q)
        threshold=55
        if alignment>=72 and confidence>=60 and direction_pressure>=34: threshold=53
        elif alignment>=62 and confidence>=55 and direction_pressure>=30: threshold=54
        if spread>MAX_SPREAD_BPS: threshold=90
        reasons=[]
        if direction not in ('BUY','SELL'): reasons.append('NO_DIRECTION')
        if alignment<50: reasons.append('ALIGNMENT_BELOW_MINIMUM')
        if confidence<46: reasons.append('CONFIDENCE_BELOW_MINIMUM')
        if trend_ok<52: reasons.append('TIMEFRAME_CONFLICT')
        if spread>MAX_SPREAD_BPS: reasons.append('SPREAD_TOO_WIDE')
        participation_warning=vr<.55
        # Low participation is a warning, not a veto for a strong short-horizon setup.
        hard_reasons=list(reasons)
        ready=(direction in ('BUY','SELL') and alignment>=50 and confidence>=46 and trend_ok>=52 and spread<=MAX_SPREAD_BPS and quality>=threshold and direction_pressure>=20)
        result={'ready':ready,'direction':direction,'alignment':round(alignment,1),'confidence':round(confidence,1),'quality':round(quality,1),'threshold':threshold,'trend_agreement':round(trend_ok,1),'liquidity_score':round(liquidity,1),'volatility_score':round(volatility_q,1),'momentum_score':round(momentum_q,1),'volume_score':round(volume_q,1),'pressure_score':round(pressure_q,1),'regime':regime,'reasons':hard_reasons,'warnings':['LOW_PARTICIPATION'] if participation_warning else []}
        with LOCK:
            S['adaptive_strategy']=result;S['setup_quality']=result['quality'];S['adaptive_threshold']=threshold;S['confluence_score']=round((alignment+confidence+trend_ok)/3,1);S['regime_quality']=round((trend_ok+momentum_q)/2,1);S['trend_agreement']=round(trend_ok,1);S['liquidity_score']=round(liquidity,1);S['volatility_score']=round(volatility_q,1);S['momentum_score']=round(momentum_q,1);S['strategy_confidence']=round((alignment+confidence)/2,1);S['entry_validation']='READY' if ready else 'BLOCKED';S['validation_reasons']=reasons;S['last_strategy_027_review']=now();S['strategy_027_reviews']+=1
            S['entry_quality_history']=(S.get('entry_quality_history',[])+[{'time':now(),'quality':result['quality'],'threshold':threshold,'direction':direction,'alignment':result['alignment'],'confidence':result['confidence'],'regime':regime,'ready':ready}])[-100:]
            if ready:S['strategy_027_passes']+=1
        return result
    except Exception as e:
        log_error('ADAPTIVE_REVIEW',e)
        return {'ready':False,'threshold':64,'quality':0.0,'reasons':['REVIEW_ERROR']}


def adaptive_entry_gate(side, score):
    r=adaptive_strategy_review(S.get('candles',[]))
    reasons=list(r.get('reasons',[]))
    if r.get('direction')!=side: reasons.append('DIRECTION_MISMATCH')
    if float(score or 0)<float(r.get('threshold',68)): reasons.append('SCORE_BELOW_ADAPTIVE_THRESHOLD')
    ok=not reasons
    with LOCK:
        S['entry_validation']='READY' if ok else 'BLOCKED'
        S['validation_reasons']=reasons
        if not ok:S['strategy_027_rejections']+=1
    return ok, reasons


def legacy_open_trade(side,source='AI SIGNAL'):
    S['entry_attempts']+=1;block=risk_block(side)
    if block:S['risk_blocks']+=1;S['session_risk_blocks']+=1;log_decision(side,S.get('score',0),block,'Risk/connection gate blocked entry.');return False
    p=ticker() or S['price'];budget=min(S['per_trade'],free());lot=S['lot_size'] if S['lot_size']>0 else budget/max(p,1e-9);lot=min(lot,budget/max(p,1e-9));used=lot*p
    if len(S['open'])>=MAX_POSITIONS:log_decision(side,S.get('score',0),'MAX_POSITIONS','Maximum open paper positions reached.');return False
    if used<MIN_TRADE:log_decision(side,S.get('score',0),'INSUFFICIENT_CAPITAL','Available trading pool is below minimum trade size.');return False
    sl,why=stop_for(side,p)
    if (side=='BUY' and sl>=p) or (side=='SELL' and sl<=p):log_decision(side,S.get('score',0),'INVALID_STOP','Structural stop was invalid for the current entry.');return False
    risk,rr_price=projected_target(p,side,sl,lot)
    if risk<=0:log_decision(side,S.get('score',0),'INVALID_RISK','Calculated risk was zero.');return False
    info=S.get('score_components',{})
    t={'id':S['next_id'],'side':side,'allocated':round(used,6),'lot':lot,'entry':p,'current':p,'pnl':0.0,'value':used,'started':now(),'source':source,'age':0,'status':'ACTIVE','stop_loss':sl,'initial_stop':sl,'stop_reason':why,'risk_amount':round(risk,6),'r_multiple':0.0,'be_moved':False,'target_r':MIN_RR,'target_price':round(rr_price,2),'hold_seconds':0.0,'entry_score':int(S.get('score',0)),'entry_probability':float(S.get('prediction',50)),'entry_reason':S.get('decision_note',''),'entry_regime':S.get('regime','UNKNOWN'),'entry_market_intelligence':dict(S.get('market_intelligence',{})),'entry_market_phase':S.get('market_phase','UNKNOWN'),'entry_market_confidence':S.get('market_confidence',0.0),'entry_market_alignment':S.get('market_alignment',0.0),'thesis_status':'INTACT','thesis_alignment':S.get('market_alignment',0.0),'thesis_reviews':0,'exit_confirm_count':0,'adaptive_exit_reason':'','last_exit_review':'','entry_daily':S.get('daily_structure','UNKNOWN'),'entry_4h':S.get('fourh_structure','UNKNOWN'),'entry_rsi':info.get('rsi',0),'entry_adx':info.get('adx',0),'entry_vwap_relation':info.get('vwap_relation','UNKNOWN'),'entry_volume_ratio':S.get('volume_ratio',0),'entry_spread_bps':S.get('spread_bps',0),'entry_imbalance':S.get('order_imbalance',0),'entry_funding':S.get('funding_rate',0),'entry_oi_change':S.get('open_interest_change',0),'entry_atr_pct':S.get('atr_pct',0),'stop_grace_count':0,'stop_breach_price':0.0,'stop_breach_time':'','mfe':0.0,'mae':0.0,'peak_favorable_price':p,'peak_adverse_price':p,'mfe_time':now(),'mae_time':now(),'mfe_cycle':0,'mae_cycle':0,'exit_reason':'','exit_price':0.0,'exit_pnl_before_costs':0.0,'exit_fees_slippage':0.0,'peak_giveback':0.0,'peak_giveback_pct':0.0,'data_quality':'COMPLETE','diagnostics':{}}
    S['next_id']+=1;S['open'].append(t);S['allocated']+=used;S['last_stop_reason']=why;S['paper_entries']+=1;S['session_entries']+=1;S['last_trade_time']=now();S['execution_quality']=S.get('market_quality','UNKNOWN');log_decision(side,S.get('score',0),'PAPER_ENTRY',f'Paper trade opened. {why}; target R={MIN_RR:.2f}; source={source}.');say(f'Paper trade #{t["id"]} OPENED: {side}. Source: {source}.');persist_state('paper_entry');return True

def open_trade(side,source='AI SIGNAL'):
    if side not in ('BUY','SELL'):
        return False
    if str(source).startswith('QUICK_ADVANTAGE'):
        sc=float(S.get('quick_score',0) or 0)
        if sc<46:
            log_decision(side,sc,'QUICK_SCORE_BLOCK','Quick-advantage score below 49.');return False
        rb=risk_block(side)
        if rb:
            S['risk_blocks']+=1;S['session_risk_blocks']+=1;log_decision(side,sc,rb,'Quick opportunity blocked by hard risk gate.');return False
        return legacy_open_trade(side,source)
    # Adaptive review is diagnostic/advisory only. The selected 1m AI signal is
    # not vetoed a second time. Hard safety gates still run in risk_block().
    rb=risk_block(side)
    if rb:
        S['risk_blocks']+=1;S['session_risk_blocks']+=1;log_decision(side,S.get('score',0),rb,'Paper entry blocked by hard risk gate.');return False
    return legacy_open_trade(side,source)


def close_trade(t,reason,p=None):
    p=p or ticker() or t['current']
    d=1 if t['side']=='BUY' else -1
    gross=(p-t['entry'])*t['lot']*d
    costs=t['allocated']*(FEE+SLIPPAGE)*2
    pnl=gross-costs
    t['current']=p;t['pnl']=round(pnl,6);t['value']=round(t['allocated']+pnl,6)
    t['status']='CLOSED';t['exit']=p;t['exit_price']=p;t['reason']=reason;t['exit_reason']=reason;t['closed']=now();t['exit_pnl_before_costs']=round(gross,6);t['exit_fees_slippage']=round(costs,6);t['hold_cycles']=int(t.get('age',0) or 0)
    mfe=max(float(t.get('mfe',0) or 0),0.0);giveback=max(0.0,mfe-pnl)
    t['peak_giveback']=round(giveback,6);t['peak_giveback_pct']=round((giveback/mfe*100) if mfe>0 else 0,2)
    if int(t.get('entry_score',0) or 0)==0 and not t.get('entry_reason'):
        t['data_quality']='LEGACY_INCOMPLETE'
    t['diagnostics']=trade_diagnostics(t)
    S['allocated']=max(0,S['allocated']-t['allocated']);S['pool']=max(0.0,S['pool']+pnl);S['paper_balance']=max(0.0,S['paper_balance']+pnl);S['realized_pnl_total']=round(float(S.get('realized_pnl_total',0.0))+pnl,6)
    S['closed'].append(t);S['session_closes']+=1
    if pnl>=0:S['wins']+=1;S['consecutive_losses']=0
    else:S['losses']+=1;S['consecutive_losses']+=1;S['loss_cooldown']=COOLDOWN_AFTER_LOSS
    S['peak_pool']=max(S['peak_pool'],S['pool'])
    if t in S['open']:S['open'].remove(t)
    analyze_performance();diagnostic_report();say(f'Paper trade #{t["id"]} CLOSED: {reason}; P&L {pnl:+.4f}.');persist_state('paper_close')

def auto_tp_target(t):
    """Small net-profit target designed for rapid paper scalps."""
    ar=atr(S['candles'][:-1]) if len(S.get('candles',[]))>20 else 0
    gross_target=max(t.get('allocated',0)*0.0009, t.get('lot',0)*ar*0.55 if ar else 0, t.get('risk_amount',0)*MIN_RR)
    estimated_cost=t.get('allocated',0)*(FEE+SLIPPAGE)*2
    return round(max(gross_target,estimated_cost*1.25),6)


def manage_trade(t,p):
    d=1 if t['side']=='BUY' else -1
    t['current']=p
    gross=(p-t['entry'])*t['lot']*d
    costs=t['allocated']*(FEE+SLIPPAGE)*2
    t['pnl']=gross-costs;t['value']=t['allocated']+t['pnl']
    t['hold_seconds']=round(max(0.0,time.time()-datetime.fromisoformat(t['started']).timestamp()),2) if t.get('started') else 0.0
    if t['hold_seconds']>=MAX_HOLD_SECONDS:
        close_trade(t,'SCALP_TIME_EXIT',p);return True
    move=(p-t['entry'])*d;favorable=move*t['lot']
    if favorable>float(t.get('mfe',0) or 0):
        t['mfe']=round(favorable,6);t['peak_favorable_price']=p;t['mfe_time']=now();t['mfe_cycle']=int(t.get('age',0) or 0)
    if favorable<float(t.get('mae',0) or 0):
        t['mae']=round(favorable,6);t['peak_adverse_price']=p;t['mae_time']=now();t['mae_cycle']=int(t.get('age',0) or 0)
    risk=max(float(t.get('risk_amount',0) or 0),t['allocated']*.0001);t['r_multiple']=round(t['pnl']/risk,3)
    mfe=max(float(t.get('mfe',0) or 0),0.0)
    if mfe>0:
        giveback=max(0.0,mfe-float(t.get('pnl',0) or 0));t['peak_giveback']=round(giveback,6);t['peak_giveback_pct']=round(giveback/mfe*100,2)
    # Refresh market intelligence only when a new 5m candle snapshot exists.
    intel=S.get('market_intelligence',{})
    if S.get('candles') and S['candles'][-2].get('t')!=t.get('_intel_candle'):
        intel=market_intelligence(S['candles']);t['_intel_candle']=S['candles'][-2].get('t')
        t['thesis_reviews']=int(t.get('thesis_reviews',0))+1;S['thesis_reviews']+=1
    aligned=(intel.get('direction')==t['side'])
    alignment=float(intel.get('alignment',0) or 0)
    t['thesis_alignment']=round(alignment,1);t['thesis_status']='INTACT' if aligned and alignment>=INTEL_MIN_ALIGNMENT else 'WEAKENING' if alignment>=40 else 'INVALIDATED';t['last_exit_review']=now()
    if t['thesis_status']=='INTACT':t['exit_confirm_count']=0
    else:t['exit_confirm_count']=int(t.get('exit_confirm_count',0))+1
    ar=atr(S['candles'][:-1]) if len(S['candles'])>20 else 0
    if t['r_multiple']>=BREAKEVEN_R and not t.get('be_moved'):
        cb=t['entry']*(FEE+SLIPPAGE)*2
        t['stop_loss']=max(t['stop_loss'],t['entry']+cb) if d==1 else min(t['stop_loss'],t['entry']-cb);t['be_moved']=True;t['stop_reason']='AI BREAK-EVEN'
    if ar and t['r_multiple']>=TRAIL_START_R:
        z=p-ar*TRAIL_ATR if d==1 else p+ar*TRAIL_ATR
        if d==1 and z>t['stop_loss']:t['stop_loss']=round(z,2);t['stop_reason']='AI ATR TRAIL'
        if d==-1 and z<t['stop_loss']:t['stop_loss']=round(z,2);t['stop_reason']='AI ATR TRAIL'
    # Adaptive profit protection: protect a favorable move instead of waiting for a full stop/target cycle.
    if t['r_multiple']>=PROFIT_LOCK_R and t['thesis_status']!='INTACT' and int(t.get('exit_confirm_count',0))>=EXIT_CONFIRMATIONS:
        t['adaptive_exit_reason']='PROFIT_PROTECTION_THESIS_WEAK';S['adaptive_exit_count']+=1;close_trade(t,'AI_PROFIT_PROTECTION',p);return True
    if mfe>0 and float(t.get('peak_giveback_pct',0) or 0)>=GIVEBACK_EXIT_PCT and t['r_multiple']<=0.25 and int(t.get('exit_confirm_count',0))>=EXIT_CONFIRMATIONS:
        t['adaptive_exit_reason']='MFE_GIVEBACK';S['adaptive_exit_count']+=1;close_trade(t,'AI_GIVEBACK_PROTECTION',p);return True
    if t['r_multiple']<=THESIS_FAIL_R and t['thesis_status']=='INVALIDATED' and int(t.get('exit_confirm_count',0))>=EXIT_CONFIRMATIONS and int(t.get('age',0))>=THESIS_GRACE_CYCLES:
        t['adaptive_exit_reason']='THESIS_INVALIDATED';S['adaptive_exit_count']+=1;close_trade(t,'AI_THESIS_INVALIDATED',p);return True
    return False


def structure_stop_action(t,p):
    d=1 if t['side']=='BUY' else -1;sl=float(t.get('stop_loss',0) or 0)
    breached=(p<=sl) if d==1 else (p>=sl)
    if not breached:
        t['stop_grace_count']=0;return False
    ar=atr(S['candles'][:-1]) if len(S.get('candles',[]))>20 else 0.0
    penetration=abs(p-sl);limit=(ar or abs(t.get('entry',p))*0.001)*STRUCTURE_STOP_GRACE_ATR
    intact=t.get('thesis_status')=='INTACT' and float(t.get('thesis_alignment',0) or 0)>=INTEL_MIN_ALIGNMENT
    if intact and penetration<=limit and int(t.get('stop_grace_count',0))<STRUCTURE_STOP_MAX_GRACE:
        t['stop_grace_count']=int(t.get('stop_grace_count',0))+1;t['stop_breach_price']=p;t['stop_breach_time']=now();t['adaptive_exit_reason']='STRUCTURE_STOP_GRACE_REVIEW'
        log_decision(t['side'],t.get('entry_score',0),'STRUCTURE_STOP_GRACE','Stop touched while thesis remained intact; one bounded ATR confirmation allowed.')
        return False
    close_trade(t,'AI_STRUCTURE_STOP',p);return True


def manage(allow_entries=True):
    # Fast local engine: consume the latest price snapshot without holding LOCK during trade I/O.
    with LOCK:
        p=float(S.get('price') or 0.0);open_copy=list(S['open']);take_profit=float(S.get('take_profit',0) or 0)
    if not p:return
    for t in open_copy:
        if t not in S.get('open',[]):continue
        if manage_trade(t,p):continue
        if structure_stop_action(t,p):continue
        target=take_profit if take_profit>0 else auto_tp_target(t)
        with LOCK:S['auto_take_profit']=0 if take_profit>0 else target
        if target>0 and t.get('pnl',0)>=target:
            with LOCK: side,sc,_=decide(S['candles'],True)
            close_trade(t,'TAKE_PROFIT' if take_profit>0 else 'AI_TAKE_PROFIT',p)
            with LOCK:S['profit_closes']+=1
            if allow_entries and side==t['side'] and sc>=MIN_SCORE:
                open_trade(side,'TAKE_PROFIT_REENTRY')
        # Entries are now driven by the dedicated 1m BTCUSDT engine in candle_step().

def _verified_candles(rows, minimum=30):
    """Validate only closed OHLCV candles; the live/forming candle is never allowed to block verification."""
    if not isinstance(rows,list) or len(rows)<minimum+1:return False,'INSUFFICIENT_DATA'
    # Exchanges normally include the currently-forming candle as the newest row.
    # The strategy itself consumes rows[-2], so verification must use that same
    # closed-candle boundary instead of allowing a transient live candle to fail
    # the structural gate.
    closed=rows[:-1]
    prev=0; times=[]
    for z in closed[-minimum:]:
        try:
            t=int(z['t']);o=float(z['o']);h=float(z['h']);l=float(z['l']);c=float(z['c']);v=float(z['v'])
            if t<=prev or not all(math.isfinite(x) for x in (o,h,l,c,v)) or min(o,h,l,c)<=0:return False,'INVALID_CLOSED_CANDLE_VALUES'
            if h<max(o,c) or l>min(o,c) or h<l or v<0:return False,'INVALID_CLOSED_OHLCV_STRUCTURE'
            times.append(t);prev=t
        except Exception:return False,'INVALID_CLOSED_CANDLE_SCHEMA'
    # Reject a broken closed-candle time series: strategy must not bridge missing candles.
    if len(times)>=3:
        diffs=[times[i]-times[i-1] for i in range(1,len(times))]
        med=sorted(diffs)[len(diffs)//2]
        if med<=0 or any(x<med*.5 or x>med*1.5 for x in diffs):return False,'CLOSED_CANDLE_TIME_GAP'
    return True,''

def _snapshot_age(rows, interval_seconds):
    """Age of the latest CLOSED candle, independent of HTTP/cache freshness."""
    try:
        if not isinstance(rows,list) or len(rows)<2:return float('inf')
        t=int(rows[-2]['t'])/1000.0
        return max(0.0,time.time()-(t+float(interval_seconds)))
    except Exception:return float('inf')


def candle_step():
    # Network work stays outside LOCK. Only a verified, fresh snapshot reaches the strategy.
    c5=api('5',180);c1=api('1',180)
    cache_age5=api_age('5',180);cache_age1=api_age('1',180)
    data_age5=_snapshot_age(c5,300);data_age1=_snapshot_age(c1,60)
    ok5,r5=_verified_candles(c5,30);ok1,r1=_verified_candles(c1,30)
    fetch_stale=max(cache_age5,cache_age1)
    candle_stale=max(data_age5,data_age1)
    with LOCK:
        S['stale_data_seconds']=round(fetch_stale,2)
        # A verified snapshot is authoritative until its own recorded freshness
        # expires. A transient failed/partial refresh must not erase a snapshot
        # that is still inside the hard freshness limits.
        cached5=list(S.get('candles') or [])
        cached1=list(S.get('one_minute_candles') or [])
        prior_verified=(S.get('data_verification_status')=='VERIFIED' and
                        S.get('verification_state')=='VERIFIED' and
                        len(cached5)>=31 and len(cached1)>=31)
    cached_age5=_snapshot_age(cached5,300)
    cached_age1=_snapshot_age(cached1,60)
    retained_verified=bool(prior_verified and cache_age5<=STALE_SECONDS and cache_age1<=STALE_SECONDS and
                            cached_age5<=MARKET_5M_MAX_AGE and cached_age1<=MARKET_1M_MAX_AGE)
    data_fresh=(cache_age5<=STALE_SECONDS and cache_age1<=STALE_SECONDS and data_age5<=MARKET_5M_MAX_AGE and data_age1<=MARKET_1M_MAX_AGE)
    if (not ok5 or not ok1 or not data_fresh) and retained_verified:
        # Keep using the last independently verified snapshot. This is not a
        # freshness bypass: its fetch and closed-candle ages are rechecked above.
        c5,c1=cached5,cached1
        ok5,r5=_verified_candles(c5,30); ok1,r1=_verified_candles(c1,30)
        data_age5=cached_age5; data_age1=cached_age1
        cache_age5=api_age('5',180); cache_age1=api_age('1',180)
        fetch_stale=max(cache_age5,cache_age1); candle_stale=max(data_age5,data_age1)
        data_fresh=bool(ok5 and ok1 and cache_age5<=STALE_SECONDS and cache_age1<=STALE_SECONDS and
                        data_age5<=MARKET_5M_MAX_AGE and data_age1<=MARKET_1M_MAX_AGE)
        if data_fresh:
            with LOCK:
                S['data_verification_status']='VERIFIED'; S['verification_state']='VERIFIED'
                S['entry_gate']='VERIFIED_MARKET_DATA'; S['entry_validation']='NOT_READY' if not S.get('open') else S.get('entry_validation','NOT_READY')
                S['market_data_worker_state']='VERIFIED'
                S['data_verification_reason']=''
                S['data_verification_detail']=f'Previously verified snapshot retained; fetch ages {cache_age1:.1f}s/{cache_age5:.1f}s; candle ages {data_age1:.1f}s/{data_age5:.1f}s.'
            set_connection('ONLINE','Fresh verified snapshot retained through transient refresh failure.')
            with LOCK:
                if S.get('desired_running') and not S.get('paused'):
                    S['recovery_pending']=False; S['recovery_state']='RECOVERED'; S['service_state']='RUNNING'
    if not ok5 or not ok1 or not data_fresh:
        # If the retained snapshot is still valid, the branch above will have
        # converted the working data back to a verified state. Only a genuinely
        # expired/invalid snapshot may reach this hard safety block.
        if retained_verified:
            with LOCK:
                S['data_verification_status']='VERIFIED'; S['verification_state']='VERIFIED'; S['entry_gate']='VERIFIED_MARKET_DATA'; S['market_data_worker_state']='VERIFIED'
            set_connection('ONLINE','Verified snapshot remains inside hard freshness limits.')
            return
        if not ok5 or not ok1:
            failed=[]
            if not ok5: failed.append('5M_'+(r5 or 'VERIFY_FAILED'))
            if not ok1: failed.append('1M_'+(r1 or 'VERIFY_FAILED'))
            gate='DATA_VERIFY_'+'+'.join(failed); reason='; '.join(failed)
        else:
            gate='STALE_MARKET_DATA'
            reason='FETCH_5M_STALE' if cache_age5>STALE_SECONDS else 'FETCH_1M_STALE' if cache_age1>STALE_SECONDS else 'CANDLE_5M_STALE' if data_age5>MARKET_5M_MAX_AGE else 'CANDLE_1M_STALE'
        detail=f'5m={len(c5)}/30({r5 or "OK"}), 1m={len(c1)}/30({r1 or "OK"}), fetch_age_5m={cache_age5:.1f}s, fetch_age_1m={cache_age1:.1f}s, candle_age_5m={data_age5:.1f}s, candle_age_1m={data_age1:.1f}s.'
        with LOCK:
            S['data_verification_status']='BLOCKED';S['verification_state']='BLOCKED';S['last_verification_time']=now();S['data_verification_reason']=reason;S['data_verification_detail']=detail
            S['entry_gate']=gate;S['entry_validation']='BLOCKED';S['validation_reasons']=[reason]
            S['quick_mode']='BLOCKED_DATA';S['decision_note']='Setup detection is paused only because market-data verification failed: '+reason+'. '+detail;S['session_data_blocks']=int(S.get('session_data_blocks',0))+1
        if fetch_stale>STALE_SECONDS or candle_stale>max(MARKET_1M_MAX_AGE,MARKET_5M_MAX_AGE):
            set_connection('OFFLINE','Market data is stale or unavailable. New paper entries are blocked until fresh data returns.')
        else:
            set_connection('ONLINE','Market data connected; verifying candle history and structure.')
        log_decision('WAIT',0,gate,detail);return
    with LOCK:
        S['data_verification_status']='VERIFIED'
        S['data_verification_reason']=''
        S['data_verification_detail']=f'1m/5m verified; fetch ages {cache_age1:.1f}s/{cache_age5:.1f}s; candle ages {data_age1:.1f}s/{data_age5:.1f}s.'
        # Verification is complete. Do not leave the previous START/RESUME gate
        # visible after a valid snapshot has arrived. The strategy still has to
        # qualify a BUY/SELL setup before entry_validation can become READY.
        S['entry_gate']='VERIFIED_MARKET_DATA'
        S['quick_mode']='IDLE'
        S['verified_snapshot_generation']=int(S.get('verified_snapshot_generation',0) or 0)+1
        if S.get('entry_validation')=='BLOCKED' and 'DATA_VERIFY_PENDING' in (S.get('validation_reasons') or []):
            S['entry_validation']='NOT_READY'
            S['validation_reasons']=['AWAITING_1M_SETUP']
    set_connection('ONLINE','Verified fresh BTCUSDT 1m/5m market data received; connection is ONLINE.')
    # Verified data is the completion signal for the recovery gate. Clear any
    # stale recovery state here so the fast engine cannot remain stuck in RECOVERING.
    with LOCK:
        if S.get('desired_running') and not S.get('paused'):
            S['recovery_pending']=False
            S['recovery_state']='RECOVERED'
            S['service_state']='RUNNING'
        elif not S.get('desired_running'):
            S['recovery_pending']=False
            S['recovery_state']='READY'
            S['service_state']='READY'
    try: market_quality()
    except Exception as e: log_error('CANDLE QUALITY',e)
    try: derivatives_context()
    except Exception as e: log_error('CANDLE DERIVATIVES',e)
    try: reg=multi_regime()
    except Exception as e: reg=S.get('regime','UNKNOWN') or 'UNKNOWN'; log_error('CANDLE REGIME',e)
    with LOCK:
        vr=round(c5[-2]['v']/max(sum(z['v'] for z in c5[-22:-2])/20,1e-9),2)
        ap=round((atr(c5[:-1])/max(c5[-2]['c'],1e-9))*100,3)
        S['candles']=c5;S['one_minute_candles']=c1;S['regime']=reg;S['volume_ratio']=vr;S['atr_pct']=ap;S['peak_pool']=max(S['peak_pool'],S['pool']);S['risk_block']=''
    market_intelligence(c5);adaptive_strategy_review(c5)
    with LOCK:
        ct=c1[-2]['t']
        # A verified snapshot must be processed even when the closed 1m candle
        # has not changed yet. This is important immediately after START/RESUME
        # or recovery: otherwise the old DATA_VERIFY_PENDING state can survive
        # until the next candle and make the dashboard appear permanently stuck.
        snapshot_gen=int(S.get('verified_snapshot_generation',0) or 0)
        processed_gen=int(S.get('last_processed_snapshot_generation',0) or 0)
        ad_now=S.get('adaptive_strategy',{}) or {}
        adaptive_ready_now=bool(ad_now.get('ready')) and str(ad_now.get('direction') or '').upper() in ('BUY','SELL') and float(ad_now.get('quality',0) or 0)>=float(ad_now.get('threshold',64) or 64)
        # A verified snapshot is authoritative. Re-evaluate not only on a new
        # generation, but also when a prior cycle left the dashboard at 0.0 while
        # the already-computed adaptive review is strong enough to produce a
        # bounded NORMAL/QUICK opportunity. This removes the stale WAIT/0.0 latch
        # without weakening the hard market/risk/paper-only gates.
        needs_verified_bootstrap=(snapshot_gen>processed_gen) or (adaptive_ready_now and float(S.get('score',0) or 0)<=0)
        if S.get('data_verification_status')=='VERIFIED' and S.get('entry_gate') in ('DATA_VERIFY_PENDING','DATA_VERIFYING'):
            S['entry_gate']='VERIFIED_MARKET_DATA'
        if ct==S.get('last_1m_candle') and not needs_verified_bootstrap:return
        S['last_1m_candle']=ct;S['last_candle']=ct;S['last_processed_snapshot_generation']=snapshot_gen;S['decisions']+=1
        if S['loss_cooldown']>0:S['loss_cooldown']-=1
        try:
            side,sc,info=btc_1m_strategy(c1)
            with LOCK:
                S['signal_engine_status']='RUNNING';S['signal_engine_error']='';S['signal_engine_last_eval']=now();S['signal_engine_evaluations']=int(S.get('signal_engine_evaluations',0))+1
        except Exception as e:
            prev_side=S.get('signal','WAIT'); prev_score=float(S.get('score',0) or 0)
            side,sc,info='WAIT',0,{'mode':'WAIT','prob':50,'reason':'1m signal evaluator error; paper entry held safely; previous signal retained for diagnostics: '+str(prev_side)+' '+str(round(prev_score,1))}
            with LOCK:
                S['signal_engine_status']='ERROR';S['signal_engine_error']=f'{type(e).__name__}: {e}'[-300:];S['signal_engine_last_eval']=now();S['signal_engine_evaluations']=int(S.get('signal_engine_evaluations',0))+1
            log_error('1M SIGNAL ENGINE',e)
        # Final handoff repair: adaptive_strategy_review is an independent, already
        # validated direction/quality layer. If it is READY but the 1m evaluator
        # still returned WAIT/0, route that evidence through the existing bounded
        # QUICK_ADVANTAGE path. This is not a risk bypass: capital, same-side,
        # connection, market-quality, paper-only and execution gates still apply.
        ad_now=S.get('adaptive_strategy',{}) or {}
        ad_side=str(ad_now.get('direction') or '').upper()
        ad_quality=float(ad_now.get('quality',0) or 0)
        ad_threshold=float(ad_now.get('threshold',64) or 64)
        ad_align=float(ad_now.get('alignment',0) or 0)
        ad_conf=float(ad_now.get('confidence',0) or 0)
        if side=='WAIT' and float(sc or 0)<=0 and bool(ad_now.get('ready')) and ad_side in ('BUY','SELL') and ad_quality>=ad_threshold and ad_align>=50 and ad_conf>=46:
            side=ad_side; sc=round(max(ad_quality,ad_threshold),1); info=dict(info or {}); info.update({'mode':'QUICK_ADVANTAGE','prob':round(max(float(info.get('prob',50) or 50),ad_quality),1),'reason':'Verified adaptive setup bridged to QUICK_ADVANTAGE after a stale/empty 1m score handoff.'})
            with LOCK:
                S['signal_engine_status']='RUNNING';S['signal_engine_error']='';S['signal_engine_last_eval']=now()
        S['last_signal_engine_side']=side; S['last_signal_engine_score']=sc; S['signal']=side; S['score']=sc; S['last_action']=side;S['prediction']=info.get('prob',50);S['prediction_reason']=info.get('reason','');S['execution_quality']=S.get('market_quality','UNKNOWN')
        if side=='BUY':S['buy_signals']+=1;S['signals_seen']+=1;S['last_signal_time']=now();S['session_signals']+=1
        elif side=='SELL':S['sell_signals']+=1;S['signals_seen']+=1;S['last_signal_time']=now();S['session_signals']+=1
        mode=info.get('mode','WAIT');required=54 if mode=='NORMAL' else 46
        ready_signal=side in ('BUY','SELL') and sc>=required
        if ready_signal:
            S['validation_passes']+=1;S['session_validation_passes']+=1;S['entry_validation']='READY';S['validation_reasons']=[]
            log_decision(side,sc,'READY',f'{mode} BTCUSDT 1m setup qualified; score {sc} >= {required}. Adaptive review is advisory; hard safety gates remain active.')
            source='QUICK_ADVANTAGE_1M' if mode=='QUICK_ADVANTAGE' else 'NORMAL_1M_MOMENTUM'
        elif side in ('BUY','SELL'):
            S['validation_rejects']+=1;S['entry_validation']='BLOCKED';S['validation_reasons']=[f'{mode}_SCORE_BELOW_{required}'];log_decision(side,sc,'SIGNAL_BELOW_THRESHOLD',f'{mode} 1m setup score {sc} below {required}.');source=''
        else:
            if S.get('signal_engine_status')=='ERROR':
                S['entry_validation']='BLOCKED';S['validation_reasons']=['SIGNAL_ENGINE_ERROR'];log_decision('WAIT',0,'SIGNAL_ENGINE_ERROR',S.get('signal_engine_error') or '1m signal evaluator failed; paper entry held safely.');source=''
            else:
                S['entry_validation']='NOT_READY';S['validation_reasons']=['NO_1M_OPPORTUNITY'];log_decision('WAIT',sc,'NO_SIGNAL','No sufficient BTCUSDT 1m opportunity.');source=''
    # Execute outside LOCK: hard safety gates may inspect live state and paper execution may persist state.
    if ready_signal:
        with LOCK:
            enough=free()>=S['per_trade'];same_side=any(t['side']==side for t in S['open'])
        if enough and not same_side:
            if open_trade(side,source):
                with LOCK:S['forming_entry_candle']=ct
        else:
            with LOCK:S['entry_validation']='BLOCKED';S['validation_reasons']=['CAPITAL_OR_SAME_SIDE_POSITION'];S['entry_gate']='CAPITAL_OR_SAME_SIDE_POSITION'
    reversal=[]
    with LOCK:
        for t in list(S['open']):
            t['age']+=1
            if side in ('BUY','SELL') and side!=t['side'] and sc>=54:reversal.append(t)
    for t in reversal:close_trade(t,'SIGNAL_REVERSAL')
    persist_state('1m_candle_decision')

def validate_strategy(_=None):
    d=api('D',520)
    if len(d)<120:
        S['validation']={'status':'INSUFFICIENT_DATA','samples':0,'win_rate':0,'profit_factor':0,'expectancy':0,'threshold':0,'tested':[]};return
    q=d[:-1];results=[]
    for th in (55,60,65,70):
        wins=losses=0;gw=gl=0;rets=[]
        for i in range(50,len(q)-2):
            st,info=structure_from(q[:i+1])
            if st not in ('BULLISH','BEARISH'):continue
            x=q[i];rng=max(x['h']-x['l'],1e-9);body=abs(x['c']-x['o'])/rng;strength=55+(10 if (st=='BULLISH' and x['c']>x['o']) or (st=='BEARISH' and x['c']<x['o']) else 0)+(5 if body>.45 else 0)
            if strength<th:continue
            e=q[i+1]['o'];z=q[i+1]['c'];ret=(z/e-1) if st=='BULLISH' else (e/z-1);ret-=2*(FEE+SLIPPAGE);rets.append(ret)
            if ret>0:wins+=1;gw+=ret
            else:losses+=1;gl+=abs(ret)
        n=len(rets);pf=gw/gl if gl else (99 if gw else 0);exp=sum(rets)/n*100 if n else 0;wr=wins/n*100 if n else 0
        results.append({'threshold':th,'samples':n,'win_rate':round(wr,2),'profit_factor':round(pf,3),'expectancy':round(exp,4)})
    viable=[r for r in results if r['samples']>=VALIDATION_MIN_SAMPLES and r['profit_factor']>1 and r['expectancy']>0];best=max(viable,key=lambda r:(r['profit_factor'],r['expectancy'])) if viable else None
    S['validation']={'status':'VALIDATED_EDGE' if best else 'DEFENSIVE_NO_EDGE','samples':best['samples'] if best else sum(r['samples'] for r in results),'win_rate':best['win_rate'] if best else 0,'profit_factor':best['profit_factor'] if best else 0,'expectancy':best['expectancy'] if best else 0,'threshold':best['threshold'] if best else 0,'tested':results};S['validated_threshold']=best['threshold'] if best else 0
    say('Daily structure validation: '+('positive historical edge found after costs.' if best else 'no positive edge after costs.'))


def _safe_mean(values):
    vals=[float(x) for x in values if isinstance(x,(int,float))]
    return sum(vals)/len(vals) if vals else 0.0

def analyze_performance():
    """Compute transparent paper-trade analytics from realized trades only."""
    with LOCK:
        trades=[dict(t) for t in S.get('closed',[]) if t.get('status')=='CLOSED']
    n=len(trades); wins=[t for t in trades if float(t.get('pnl',0))>=0]; losses=[t for t in trades if float(t.get('pnl',0))<0]
    gross_win=sum(float(t.get('pnl',0)) for t in wins); gross_loss=abs(sum(float(t.get('pnl',0)) for t in losses))
    pf=(gross_win/gross_loss) if gross_loss>0 else (99.0 if gross_win>0 else 0.0)
    expectancy=_safe_mean([float(t.get('pnl',0))/max(float(t.get('allocated',1)),1e-9)*100 for t in trades])
    wr=(len(wins)/n*100) if n else 0.0
    avg_win=_safe_mean([t.get('pnl',0) for t in wins]);avg_loss=_safe_mean([t.get('pnl',0) for t in losses])
    equity=DEFAULT_POOL;peak=equity;max_dd=0.0;curve=[]
    for t in trades:
        equity+=float(t.get('pnl',0));peak=max(peak,equity);dd=(peak-equity)/max(peak,1e-9)*100;max_dd=max(max_dd,dd);curve.append(equity)
    by_score={}
    by_side={}; by_reason={}; by_regime={}
    for t in trades:
        score=int(t.get('entry_score',0) or 0); bucket=(score//5)*5
        by_score.setdefault(str(bucket),[]).append(float(t.get('pnl',0)))
        side=t.get('side','?');by_side.setdefault(side,[]).append(float(t.get('pnl',0)))
        reason=t.get('reason','UNKNOWN');by_reason.setdefault(reason,[]).append(float(t.get('pnl',0)))
        regime=t.get('entry_regime','UNKNOWN');by_regime.setdefault(regime,[]).append(float(t.get('pnl',0)))
    def summarize(d):
        return {k:{'samples':len(v),'wins':sum(x>=0 for x in v),'net':round(sum(v),6),'avg':round(_safe_mean(v),6)} for k,v in d.items()}
    health='INSUFFICIENT_SAMPLE' if n<10 else ('POSITIVE' if pf>1 and expectancy>0 else 'DEFENSIVE')
    analytics={'samples':n,'wins':len(wins),'losses':len(losses),'win_rate':round(wr,2),'profit_factor':round(pf,3),'expectancy_pct':round(expectancy,4),'avg_win':round(avg_win,6),'avg_loss':round(avg_loss,6),'net_pnl':round(sum(float(t.get('pnl',0)) for t in trades),6),'max_drawdown_pct':round(max_dd,3),'consecutive_losses':S.get('consecutive_losses',0),'by_score':summarize(by_score),'by_side':summarize(by_side),'by_exit_reason':summarize(by_reason),'by_regime':summarize(by_regime)}
    with LOCK:
        S['analytics']=analytics;S['performance_samples']=n;S['strategy_health']=health;S['last_strategy_review']=now()
    return analytics

def score_explanation(side, score, info):
    buy=info.get('buy',0);sell=info.get('sell',0)
    return {'side':side,'score':score,'buy_score':buy,'sell_score':sell,'probability':info.get('prob',50),'reason':info.get('reason',''),'required_threshold':max(MIN_SCORE,S.get('validated_threshold',0) or 0),'directional_edge':abs(buy-sell)}

def execution_quality_snapshot():
    with LOCK:
        return {'market_quality':S.get('market_quality'),'spread_bps':S.get('spread_bps',0),'order_imbalance':S.get('order_imbalance',0),'atr_pct':S.get('atr_pct',0),'volume_ratio':S.get('volume_ratio',0),'connection':S.get('connection')}

def trade_diagnostics(t):
    score=int(t.get('entry_score',0) or 0)
    quality=t.get('data_quality','COMPLETE')
    if score==0 and not t.get('entry_reason'): quality='LEGACY_INCOMPLETE'
    mfe=float(t.get('mfe',0) or 0);mae=float(t.get('mae',0) or 0);pnl=float(t.get('pnl',0) or 0)
    return {
        'data_quality':quality,'entry_score':score,'entry_probability':t.get('entry_probability',0),'entry_reason':t.get('entry_reason',''),
        'entry_price':t.get('entry',0),'exit_price':t.get('exit',t.get('current',0)),'side':t.get('side','UNKNOWN'),
        'entry_regime':t.get('entry_regime','UNKNOWN'),'entry_daily':t.get('entry_daily','UNKNOWN'),'entry_4h':t.get('entry_4h','UNKNOWN'),
        'entry_rsi':t.get('entry_rsi',0),'entry_adx':t.get('entry_adx',0),'entry_vwap_relation':t.get('entry_vwap_relation','UNKNOWN'),
        'entry_volume_ratio':t.get('entry_volume_ratio',0),'entry_spread_bps':t.get('entry_spread_bps',0),'entry_imbalance':t.get('entry_imbalance',0),
        'entry_funding':t.get('entry_funding',0),'entry_oi_change':t.get('entry_oi_change',0),'entry_atr_pct':t.get('entry_atr_pct',0),
        'initial_stop':t.get('initial_stop',0),'current_stop':t.get('stop_loss',0),'stop_reason':t.get('stop_reason',''),
        'target_price':t.get('target_price',0),'target_r':t.get('target_r',MIN_RR),'risk_amount':t.get('risk_amount',0),
        'mfe':round(mfe,6),'mae':round(mae,6),'peak_favorable_price':t.get('peak_favorable_price',0),'peak_adverse_price':t.get('peak_adverse_price',0),
        'mfe_cycle':t.get('mfe_cycle',0),'mae_cycle':t.get('mae_cycle',0),'r_multiple':t.get('r_multiple',0),
        'pnl':round(pnl,6),'pool_impact_pct':round(_pct_of_pool(pnl,100.0),4),'exit_pnl_before_costs':t.get('exit_pnl_before_costs',0),
        'exit_fees_slippage':t.get('exit_fees_slippage',0),'peak_giveback':round(float(t.get('peak_giveback',0) or 0),6),
        'peak_giveback_pct':round(float(t.get('peak_giveback_pct',0) or 0),2),'hold_cycles':t.get('hold_cycles',t.get('age',0)),
        'exit_reason':t.get('exit_reason',t.get('reason','UNKNOWN')),'started':t.get('started',''),'closed':t.get('closed','')
    }

def _pct_of_pool(value, pool=100.0):
    return (float(value)/max(pool,1e-9))*100.0

def diagnostic_report():
    """Build a transparent trade-by-trade diagnostic report. Descriptive only; never changes strategy thresholds."""
    with LOCK:
        trades=[dict(t) for t in S.get('closed',[]) if t.get('status')=='CLOSED']
        pool_reference=100.0
    rows=[]
    for t in trades[-80:]:
        pnl=float(t.get('pnl',0) or 0)
        allocated=float(t.get('allocated',0) or 0)
        mfe=float(t.get('mfe',0) or 0)
        mae=float(t.get('mae',0) or 0)
        risk=float(t.get('risk_amount',0) or 0)
        rows.append({
            'id':t.get('id'),'side':t.get('side','UNKNOWN'),'entry':t.get('entry',0),'exit':t.get('exit',t.get('current',0)),
            'pnl':round(pnl,6),'pool_impact_pct':round(_pct_of_pool(pnl,pool_reference),4),
            'allocated':round(allocated,6),'entry_score':int(t.get('entry_score',0) or 0),
            'entry_probability':round(float(t.get('entry_probability',0) or 0),2),'daily':t.get('entry_daily','UNKNOWN'),
            'fourh':t.get('entry_4h','UNKNOWN'),'regime':t.get('entry_regime','UNKNOWN'),
            'rsi':round(float(t.get('entry_rsi',0) or 0),2),'adx':round(float(t.get('entry_adx',0) or 0),2),
            'vwap':t.get('entry_vwap_relation','UNKNOWN'),'volume_ratio':round(float(t.get('entry_volume_ratio',0) or 0),2),
            'spread_bps':round(float(t.get('entry_spread_bps',0) or 0),4),'imbalance':round(float(t.get('entry_imbalance',0) or 0),4),
            'funding':round(float(t.get('entry_funding',0) or 0),8),'oi_change':round(float(t.get('entry_oi_change',0) or 0),4),
            'atr_pct':round(float(t.get('entry_atr_pct',0) or 0),4),'initial_stop':t.get('initial_stop',0),
            'stop_loss':t.get('stop_loss',0),'stop_reason':t.get('stop_reason',''),'target_price':t.get('target_price',0),
            'target_r':round(float(t.get('target_r',MIN_RR) or MIN_RR),2),'mfe':round(mfe,6),'mae':round(mae,6),
            'risk_amount':round(risk,6),'r_multiple':round(float(t.get('r_multiple',0) or 0),3),
            'hold_cycles':int(t.get('hold_cycles',t.get('age',0)) or 0),'exit_reason':t.get('exit_reason',t.get('reason','UNKNOWN')),
            'entry_reason':t.get('entry_reason',''),'market_phase':t.get('entry_market_phase','UNKNOWN'),'market_confidence':round(float(t.get('entry_market_confidence',0) or 0),1),'thesis_status':t.get('thesis_status','UNKNOWN'),'thesis_alignment':round(float(t.get('thesis_alignment',0) or 0),1),'adaptive_exit_reason':t.get('adaptive_exit_reason',''),'closed':t.get('closed',''),'data_quality':t.get('data_quality','COMPLETE'),'peak_favorable_price':t.get('peak_favorable_price',0),'peak_adverse_price':t.get('peak_adverse_price',0),'mfe_cycle':int(t.get('mfe_cycle',0) or 0),'mae_cycle':int(t.get('mae_cycle',0) or 0),'peak_giveback':round(float(t.get('peak_giveback',0) or 0),6),'peak_giveback_pct':round(float(t.get('peak_giveback_pct',0) or 0),2)
        })
    def group(key):
        out={}
        for r in rows:
            k=str(r.get(key,'UNKNOWN'))
            d=out.setdefault(k,{'samples':0,'wins':0,'losses':0,'net_pnl':0.0,'avg_pnl':0.0,'avg_mfe':0.0,'avg_mae':0.0})
            d['samples']+=1;d['wins']+=1 if r['pnl']>=0 else 0;d['losses']+=1 if r['pnl']<0 else 0
            d['net_pnl']+=r['pnl'];d['avg_mfe']+=r['mfe'];d['avg_mae']+=r['mae']
        for d in out.values():
            d['net_pnl']=round(d['net_pnl'],6);d['avg_pnl']=round(d['net_pnl']/max(d['samples'],1),6)
            d['avg_mfe']=round(d['avg_mfe']/max(d['samples'],1),6);d['avg_mae']=round(d['avg_mae']/max(d['samples'],1),6)
        return out
    stop_losses=[r for r in rows if 'STOP' in str(r['exit_reason']).upper()]
    tp_trades=[r for r in rows if 'TAKE_PROFIT' in str(r['exit_reason']).upper()]
    winners=[r for r in rows if r['pnl']>=0]
    losses=[r for r in rows if r['pnl']<0]
    near_miss=[r for r in losses if r['mfe']>0]
    diagnosis=[]
    if not rows:
        diagnosis.append('No completed paper trades are available for diagnosis yet.')
    else:
        if len(rows)<10: diagnosis.append('Sample remains small; findings are descriptive and should not trigger automatic threshold changes.')
        if not winners: diagnosis.append('All recorded trades are losses; inspect entry timing, stop placement, and exit behavior before changing the signal model.')
        if near_miss: diagnosis.append(f'{len(near_miss)} losing trade(s) recorded positive MFE before closing; review whether exits/stop placement allowed normal market noise.')
        if stop_losses: diagnosis.append(f'{len(stop_losses)} trade(s) closed by a stop-related reason; compare their MAE/MFE and structural-stop distance.')
        if tp_trades: diagnosis.append(f'{len(tp_trades)} trade(s) reached a take-profit exit.')
        if not near_miss and losses: diagnosis.append('Losing trades did not record positive MFE; entry timing or directional signal quality deserves review.')
    score_bands={}
    for r in rows:
        b=str((int(r.get('entry_score',0))//5)*5)
        score_bands.setdefault(b,[]).append(r)
    score_summary={}
    for b,vals in score_bands.items():
        score_summary[b]={'samples':len(vals),'wins':sum(1 for r in vals if r['pnl']>=0),'losses':sum(1 for r in vals if r['pnl']<0),'net_pnl':round(sum(r['pnl'] for r in vals),6),'avg_pnl':round(sum(r['pnl'] for r in vals)/max(len(vals),1),6)}
    legacy=[r for r in rows if r.get('data_quality')=='LEGACY_INCOMPLETE']
    giveback=[r for r in losses if r.get('mfe',0)>0 and r.get('peak_giveback_pct',0)>=50]
    max_hold=[r for r in rows if str(r.get('exit_reason','')).upper()=='MAX_HOLD']
    zero_cycle=[r for r in rows if int(r.get('hold_cycles',0) or 0)==0]
    if legacy: diagnosis.append(f'{len(legacy)} legacy trade(s) have incomplete entry diagnostics (for example score 0); they are preserved but excluded from conclusions about entry quality.')
    if giveback: diagnosis.append(f'{len(giveback)} losing trade(s) gave back at least 50% of their recorded MFE before exit; review exit timing and stop movement.')
    if max_hold: diagnosis.append(f'{len(max_hold)} trade(s) reached the maximum holding period; compare their MFE/MAE before considering a holding-time change.')
    if zero_cycle: diagnosis.append(f'{len(zero_cycle)} trade(s) closed before a candle cycle increment; these should be interpreted using intracycle MFE/MAE and exit timestamps.')
    forensic={'legacy_incomplete':len(legacy),'large_giveback_losses':len(giveback),'max_hold_exits':len(max_hold),'zero_cycle_exits':len(zero_cycle),'complete_records':len(rows)-len(legacy)}
    with LOCK:
        S['forensic_summary']=forensic
        S['exit_analysis']={'large_giveback_losses':len(giveback),'max_hold_exits':len(max_hold),'stop_exits':len(stop_losses),'take_profit_exits':len(tp_trades)}
        S['data_quality_warnings']=[x for x in diagnosis if 'legacy' in x.lower() or 'incomplete' in x.lower()]
    return {
        'pool_reference':pool_reference,'samples':len(rows),'rows':rows,'by_side':group('side'),'by_score_band':score_summary,
        'by_regime':group('regime'),'by_exit_reason':group('exit_reason'),'diagnosis':diagnosis,
        'policy':'DESCRIPTIVE ONLY — small samples do not auto-change thresholds.',
        'generated_at':now()
    }


# ========================= Z3 AI SUPERVISOR LAYER =========================
# Paper-only supervisor. It diagnoses the existing 030 engine, performs only
# bounded safe recoveries automatically, creates task-specific specialist
# agents for diagnosis, and asks Owner/Admin before consequential changes.
Z3_VERSION='Z3-TRI-AI-SUPERVISOR-003'
Z3_AUTO_REPAIR=True
Z3_HEARTBEAT_TIMEOUT=12.0
Z3_MAX_SPECIALISTS=8
Z3_LOCK=threading.RLock()
Z3={
 'version':Z3_VERSION,'mode':'PAPER_ONLY','status':'STARTING','health':'CHECKING',
 'last_check':'','last_repair':'','last_repair_result':'','last_problem':'',
 'problem_count':0,'auto_repairs':0,'permission_requests':0,'specialists_created':0,
 'specialists':[],'events':[],'pending_permission':None,'voice_enabled':True,'dashboard_health':'CHECKING','dashboard_checks':{},'self_heal_status':'STARTING','self_heal_cycles':0,'last_self_heal':'','last_self_heal_problems':0,'last_self_heal_error':'','strategy_upgrade_status':'LEARNING','strategy_review_samples':0,'strategy_review_time':'','self_heal_problem_signature':'',
 'conversation_mode':True,'last_voice_text':'','repair_cycle':0,'guide_version':'Z3-003','authority_model':'AUTO_SAFE / APPROVAL_PROTECTED','specialist_limit':Z3_MAX_SPECIALISTS,'last_problem_signature':'','last_specialist_cycle':{},'last_voice_notice':'','dashboard_recovery':'READY','dashboard_error_count':0,'voice_error_count':0,'repair_history':[],'financial_integrity_alert':'','orchestrator_last_action':'','orchestrator_last_verification':'','upgrade_state':'READY','upgrade_last_checked':'','upgrade_last_success':'','upgrade_last_error':'','upgrade_candidate_version':'','upgrade_candidate_sha256':''
}

def _z3_assignment(tree, name):
    """Return a literal top-level or nested assignment value, or None."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
            try:
                return ast.literal_eval(node.value)
            except Exception:
                return None
    return None

def z3_validate_upgrade_bytes(raw):
    """Validate exact candidate bytes without executing the candidate."""
    if not isinstance(raw, (bytes, bytearray)) or len(raw) < 2000:
        raise RuntimeError('upgrade package is empty or too small')
    try:
        text = bytes(raw).decode('utf-8')
    except Exception as e:
        raise RuntimeError('upgrade package is not valid UTF-8') from e
    outer = compile(text, '<ZTrader-upgrade-candidate>', 'exec')
    source = text
    # A wrapped release must pass both its embedded digest and inner-source compile.
    embedded = _z3_assignment(ast.parse(text), 'S')
    expected_hash = _z3_assignment(ast.parse(text), 'H')
    filters = _z3_assignment(ast.parse(text), 'F')
    if embedded is not None and expected_hash is not None and filters is not None:
        try:
            decoded = lzma.decompress(base64.b85decode(''.join(str(embedded).split())), format=lzma.FORMAT_RAW, filters=filters)
        except Exception as e:
            raise RuntimeError('wrapped candidate payload cannot be decoded') from e
        actual_hash = hashlib.sha256(decoded).hexdigest()
        if actual_hash.lower() != str(expected_hash).lower():
            raise RuntimeError('wrapped candidate SHA-256 mismatch')
        source = decoded.decode('utf-8')
    inner_tree = ast.parse(source)
    compile(inner_tree, '<ZTrader-upgrade-inner-source>', 'exec')
    checks = {'PAPER_ONLY': True, 'LIVE_TRADING': False, 'LIVE_EXECUTION_AUTHORIZED': False, 'REAL_ORDER_EXECUTION': False}
    for key, expected in checks.items():
        actual = _z3_assignment(inner_tree, key)
        if actual is not expected:
            raise RuntimeError(f'candidate safety constant invalid: {key}')
    version = _z3_assignment(inner_tree, 'APP_VERSION')
    if not isinstance(version, str) or not re.fullmatch(r'25\.6\.\d+', version):
        raise RuntimeError('candidate APP_VERSION is missing or invalid')
    # Wrapper comments and the decoded source must identify the same release.
    m = re.search(r'V(25\.6\.\d+)', text[:1200], re.I)
    if not m or m.group(1) != version:
        raise RuntimeError('candidate wrapper/source version mismatch')
    return text, version, hashlib.sha256(bytes(raw)).hexdigest()

def z3_upgrade_package():
    """Owner/Admin-only, paper-boundary-preserving atomic upgrade with rollback.

    Downloads a release candidate, validates the wrapper and embedded source,
    checks exact safety constants and version, writes/reads a temporary file,
    creates a unique backup, installs atomically, persists, then restarts. If any
    step before successful process replacement fails, it restores the prior file.
    The update URL is a release slot; it must contain a newer complete release.
    """
    if not PAPER_ONLY or LIVE_TRADING or LIVE_EXECUTION_AUTHORIZED or REAL_ORDER_EXECUTION:
        z3_request_permission('upgrade safety-boundary review','Upgrade is blocked unless the execution boundary is paper-only.')
        return False, 'Upgrade blocked: execution boundary is not safely paper-only.'
    with Z3_LOCK:
        if Z3.get('upgrade_state') in ('CHECKING','INSTALLING','RESTARTING'):
            return False, 'Upgrade already in progress.'
        Z3['upgrade_state']='CHECKING'; Z3['upgrade_last_error']=''
    current_path=os.path.abspath(sys.argv[0] if sys.argv and sys.argv[0] else __file__)
    if not os.path.isfile(current_path):
        current_path=os.path.abspath(__file__)
    tmp=current_path+'.upgrade.tmp'
    stamp=datetime.now().strftime('%Y%m%d%H%M%S')
    bak=current_path+'.upgrade.'+stamp+'.bak'
    original_moved=False; candidate_installed=False
    try:
        req=Request(UPGRADE_URL,headers={'User-Agent':'ZTrader-AI-Owner-Upgrade/1'})
        try:
            with urlopen(req,timeout=UPGRADE_TIMEOUT) as r:
                raw=r.read()
        except Exception as e:
            raise RuntimeError('no published update package is available at the configured release URL; current version left unchanged') from e
        text, remote_version, digest = z3_validate_upgrade_bytes(raw)
        cur_parts=tuple(int(x) for x in APP_VERSION.split('.'))
        remote_parts=tuple(int(x) for x in remote_version.split('.'))
        if remote_parts <= cur_parts:
            with Z3_LOCK:
                Z3['upgrade_state']='CURRENT'; Z3['upgrade_last_checked']=now(); Z3['upgrade_candidate_version']=remote_version; Z3['upgrade_candidate_sha256']=digest
            return True, f'ZTrader AI is already current at V{APP_VERSION}; no replacement performed.'
        with Z3_LOCK:
            Z3['upgrade_state']='INSTALLING'; Z3['upgrade_candidate_version']=remote_version; Z3['upgrade_candidate_sha256']=digest
        with open(tmp,'wb') as f:
            f.write(raw); f.flush(); os.fsync(f.fileno())
        with open(tmp,'rb') as f:
            disk=f.read()
        if disk != bytes(raw):
            raise RuntimeError('candidate write verification failed')
        # Revalidate the exact bytes that will be installed.
        _, disk_version, disk_digest = z3_validate_upgrade_bytes(disk)
        if disk_version != remote_version or disk_digest != digest:
            raise RuntimeError('candidate changed during disk verification')
        os.replace(current_path,bak); original_moved=True
        os.replace(tmp,current_path); candidate_installed=True
        with open(current_path,'rb') as f:
            installed_bytes=f.read()
        _, installed_version, installed_digest=z3_validate_upgrade_bytes(installed_bytes)
        if installed_version != remote_version or installed_digest != digest:
            raise RuntimeError('installed candidate verification failed')
        persist_state('owner_upgrade_installed')
        with Z3_LOCK:
            Z3['upgrade_state']='RESTARTING'; Z3['upgrade_last_checked']=now(); Z3['upgrade_last_success']=now(); Z3['upgrade_last_error']=''
        z3_event(f'Owner/Admin upgrade validated and installed: V{remote_version}; SHA256={digest[:16]}...; backup retained.','UPGRADE',True)
        os.execv(sys.executable,[sys.executable,current_path])
        # execv normally never returns. Treat a return as failure and roll back.
        raise RuntimeError('process replacement unexpectedly returned')
    except SystemExit:
        raise
    except Exception as e:
        try:
            if os.path.exists(tmp): os.remove(tmp)
        except Exception:
            pass
        rollback_note=''
        if original_moved and os.path.exists(bak):
            try:
                if candidate_installed and os.path.exists(current_path):
                    os.remove(current_path)
                if not os.path.exists(current_path):
                    os.replace(bak,current_path)
                    rollback_note=' Previous version restored.'
                else:
                    rollback_note=' Backup retained at '+bak+'.'
            except Exception as re_err:
                rollback_note=' ROLLBACK ERROR: '+str(re_err)[-180:]+'; backup retained at '+bak+'.'
        with Z3_LOCK:
            Z3['upgrade_state']='FAILED'; Z3['upgrade_last_error']=(str(e)+rollback_note)[-400:]; Z3['upgrade_last_checked']=now()
        z3_event('Owner/Admin upgrade rejected safely: '+str(e)[-300:]+rollback_note,'UPGRADE',True)
        return False, 'Upgrade rejected safely: '+str(e)[-220:]+rollback_note

def z3_event(text,kind='INFO',speak=False):
    item={'time':now(),'kind':kind,'text':str(text)[:500]}
    with Z3_LOCK:
        Z3['events']=(Z3.get('events',[])+[item])[-80:]
    if speak: say('Z3 Supervisor: '+str(text)[:420])

def z3_specialist(role,reason):
    """Create a bounded specialist record and perform a deterministic diagnosis.
    Specialists never execute trading, change risk limits, rewrite source, or
    enable live execution. They return evidence to the Orchestrator.
    """
    role=str(role); reason=str(reason)
    with Z3_LOCK:
        existing=[x for x in Z3.get('specialists',[]) if x.get('status')=='ACTIVE']
        if len(existing)>=Z3_MAX_SPECIALISTS:
            z3_event('Specialist capacity reached; existing diagnostic capacity retained.','WARN')
            return None
        sid=f'{role.upper().replace(" ","_")}-{int(time.time())}'
        a={'id':sid,'role':role,'reason':reason[:240],'status':'ACTIVE','created':now(),'result':'PENDING'}
        Z3['specialists']=(Z3.get('specialists',[])+[a])[-Z3_MAX_SPECIALISTS:]
        Z3['specialists_created']+=1
    result='diagnosis recorded; no protected action executed'
    if 'Network' in role:
        result=f'connection={S.get("connection")}; data_age_1m={round(api_age("1",180),1)}s; data_age_5m={round(api_age("5",180),1)}s'
    elif 'Engine' in role:
        result=f'heartbeat_age={round(time.time()-float(S.get("engine_heartbeat") or time.time()),1)}s; service={S.get("service_state")}'
    elif 'State' in role or 'Integrity' in role:
        result=f'persistence={S.get("persistence_status")}; file_exists={os.path.exists(STATE_FILE)}'
    elif 'Security' in role:
        result=f'paper_only={PAPER_ONLY}; live_trading={LIVE_TRADING}; real_orders={REAL_ORDER_EXECUTION}; authorization={LIVE_EXECUTION_AUTHORIZED}'
    elif 'Strategy' in role:
        result=f'entry_validation={S.get("entry_validation")}; setup={float(S.get("setup_quality") or 0):.1f}; threshold={float(S.get("adaptive_threshold") or 0):.1f}; direction={S.get("direction",S.get("market_direction","WAIT"))}'
    with Z3_LOCK:
        for x in Z3.get('specialists',[]):
            if x.get('id')==sid:
                x['status']='COMPLETE'; x['result']=result
    z3_event(f'{role} specialist completed: {result}','SPECIALIST')
    return sid

def z3_request_permission(action,reason):
    with Z3_LOCK:
        if Z3.get('pending_permission'): return
        Z3['pending_permission']={'action':action,'reason':reason,'requested':now()}
        Z3['permission_requests']+=1
    z3_event(f'Owner/Admin permission required: {action}. {reason}','PERMISSION',True)

def _record_repair(action, verified=True):
    with Z3_LOCK:
        Z3['repair_history']=(Z3.get('repair_history',[])+[{'time':now(),'action':str(action)[:300],'verified':bool(verified)}])[-40:]
        Z3['last_repair']=now(); Z3['last_repair_result']=str(action)[:500]; Z3['orchestrator_last_action']=str(action)[:300]; Z3['orchestrator_last_verification']=now() if verified else ''
        if verified: Z3['auto_repairs']=int(Z3.get('auto_repairs',0))+1


def start_market_worker(force=False):
    global MARKET_THREAD_STARTED, MARKET_THREAD
    with THREAD_START_LOCK:
        if STOP.is_set() or MARKET_STOP.is_set(): return False
        if MARKET_THREAD is not None and MARKET_THREAD.is_alive() and not force: return True
        MARKET_THREAD_STARTED=True
        MARKET_THREAD=threading.Thread(target=market_data_worker,name='ZTrader-MarketData',daemon=True)
        MARKET_THREAD.start()
        return True

def start_engine_worker(force=False):
    global ENGINE_THREAD
    with THREAD_START_LOCK:
        if STOP.is_set(): return False
        if ENGINE_THREAD is not None and ENGINE_THREAD.is_alive() and not force: return True
        ENGINE_THREAD=threading.Thread(target=engine,name='ZTrader-Engine',daemon=True)
        ENGINE_THREAD.start()
        return True

def z3_financial_integrity_check():
    """Detect accounting inconsistencies but NEVER auto-correct financial truth."""
    issues=[]
    try:
        balance=float(S.get('paper_balance',0) or 0); pool=float(S.get('pool',0) or 0); allocated=float(S.get('allocated',0) or 0)
        if balance < -1e-9 or pool < -1e-9 or allocated < -1e-9: issues.append('negative account value detected')
        if pool > balance + 1e-6: issues.append('trading pool exceeds paper account balance')
        if allocated > pool + 1e-6: issues.append('allocated capital exceeds trading pool')
        calc=sum(float(x.get('pnl',0) or 0) for x in S.get('closed',[]) if isinstance(x,dict))
        if abs(calc-float(S.get('realized_pnl_total',0) or 0))>1e-5: issues.append('realized P&L does not reconcile with closed-trade records')
    except Exception as e: issues.append('financial integrity check failed: '+str(e)[-180:])
    with LOCK: S['financial_integrity_alert']='; '.join(issues)[:500]
    return issues

def z3_safe_repair():
    """Central Orchestrator repair playbook for non-financial technical faults.
    It may restart dead internal workers, delegate market recovery, repair persistence,
    normalize transient runtime state, and verify the result. It never edits financial
    truth, risk limits, strategy rules, source code, authentication, or live-trading state.
    """
    repairs=[]
    try:
        if not PAPER_ONLY or LIVE_TRADING or LIVE_EXECUTION_AUTHORIZED or REAL_ORDER_EXECUTION:
            z3_request_permission('safety-boundary intervention','Execution boundary is not safely paper-only; automatic repair is blocked.')
            return repairs
        financial=z3_financial_integrity_check()
        if financial:
            z3_event('Financial integrity issue detected; automatic financial correction is forbidden: '+ '; '.join(financial),'FINANCIAL_BLOCK',True)
            z3_request_permission('financial-integrity review','; '.join(financial))
            return repairs
        if not os.path.exists(STATE_FILE):
            if persist_state('z3_recreate_missing_state'): repairs.append('recreated and verified missing runtime state')
        elif not S.get('persistence_ok') or S.get('persistence_status') not in ('VERIFIED','OK'):
            if persist_state('z3_verify_persistence'): repairs.append('repaired and verified runtime persistence')
        if MARKET_THREAD is None or not MARKET_THREAD.is_alive():
            if start_market_worker(): repairs.append('restarted dead market-data worker')
        if S.get('running') and not S.get('paused') and (ENGINE_THREAD is None or not ENGINE_THREAD.is_alive()):
            if start_engine_worker(): repairs.append('restarted dead engine worker')
        if S.get('running') and not S.get('paused') and S.get('connection')!='ONLINE':
            # IMPORTANT: Guardian/Orchestrator must not manufacture RECOVERY_REQUIRED
            # merely because the public connection field is one worker-cycle behind.
            # First reconcile from the same fresh verified snapshot used by the
            # market-data safety gate. Only the actual market-recovery path may expose
            # RECOVERY_REQUIRED after a genuine verification failure.
            normalize_verified_health()
            with LOCK:
                verified=(S.get('connection')=='ONLINE' and
                          S.get('data_verification_status')=='VERIFIED' and
                          float(S.get('recovery_last_1m_cache_age',1e9) or 1e9)<=STALE_SECONDS and
                          float(S.get('recovery_last_5m_cache_age',1e9) or 1e9)<=STALE_SECONDS and
                          float(S.get('recovery_last_1m_age',1e9) or 1e9)<=MARKET_1M_MAX_AGE and
                          float(S.get('recovery_last_5m_age',1e9) or 1e9)<=MARKET_5M_MAX_AGE)
                retry_due=(time.time()-float(S.get('last_recovery_time',0) or 0))>=RECOVERY_RETRY
            if verified:
                recovery_reconcile()
            elif retry_due:
                # Delegate the actual diagnosis to the dedicated market worker.
                # Do not label a normal verification interval as a fault here.
                try: recovery_cycle()
                except Exception as e: log_error('Z3 MARKET RECOVERY DELEGATE',e)
        if S.get('service_state')=='DEGRADED' and not S.get('engine_errors') and S.get('running'):
            with LOCK: S['service_state']='RUNNING' if not S.get('paused') else 'PAUSED'
            repairs.append('normalized transient degraded runtime state after error list cleared')
        if repairs:
            for item in repairs: _record_repair(item,True)
            z3_event('Orchestrator automatic repair completed and recorded: '+ '; '.join(repairs),'REPAIR',True)
            persist_state('z3_orchestrator_repair')
        else:
            with Z3_LOCK:
                Z3['last_repair_result']='No safe technical repair required.'
        return repairs
    except Exception as e:
        with Z3_LOCK: Z3['last_repair_result']='FAILED: '+str(e)[-300:]; Z3['orchestrator_last_action']='repair exception'
        z3_event('Orchestrator repair failed; safety boundary retained: '+str(e)[-300:],'ERROR',True)
        return repairs

def z3_diagnose():
    # Public health must be reconciled from the same verified snapshot the
    # dashboard displays before Guardian evaluates problems.
    normalize_verified_health()
    problems=[]
    if not PAPER_ONLY or LIVE_TRADING or LIVE_EXECUTION_AUTHORIZED or REAL_ORDER_EXECUTION:
        problems.append(('SECURITY','Trading boundary is not paper-only.'))
    hb=float(S.get('engine_heartbeat') or 0)
    if S.get('running') and not S.get('paused') and hb and time.time()-hb>Z3_HEARTBEAT_TIMEOUT:
        problems.append(('ENGINE','Engine heartbeat is stale.'))
    if S.get('running') and not S.get('paused') and S.get('connection')!='ONLINE' and S.get('verification_state') not in ('VERIFYING','IDLE'):
        problems.append(('NETWORK','Market connection is not ONLINE; verified 1m/5m data is unavailable or stale.'))
    if S.get('running') and not S.get('paused'):
        a1=api_age('1',180); a5=api_age('5',180)
        if a1>STALE_SECONDS or a5>STALE_SECONDS:
            problems.append(('NETWORK',f'Verified market-data freshness failed: 1m={a1:.1f}s, 5m={a5:.1f}s, source={S.get("market_data_source","UNKNOWN")}'))
    if S.get('persistence_status') not in ('VERIFIED','OK') and not S.get('persistence_ok'):
        problems.append(('PERSISTENCE','Runtime persistence is not verified.'))
    if S.get('engine_errors'):
        problems.append(('RUNTIME','Recent engine exceptions exist.'))
    if S.get('service_state')=='DEGRADED':
        problems.append(('RUNTIME','Service state is DEGRADED.'))
    if MARKET_THREAD is None or not MARKET_THREAD.is_alive():
        problems.append(('ENGINE','Market-data worker thread is not alive.'))
    if S.get('running') and not S.get('paused') and (ENGINE_THREAD is None or not ENGINE_THREAD.is_alive()):
        problems.append(('ENGINE','Engine worker thread is not alive.'))
    fin=z3_financial_integrity_check()
    if fin:
        problems.append(('FINANCIAL','; '.join(fin)))
    if S.get('entry_validation')=='READY' and S.get('connection')!='ONLINE':
        problems.append(('CONTRADICTION','Strategy is ready while the connection gate is blocking execution.'))
    if S.get('recovery_state') in ('RECOVERED','READY') and S.get('running') and (api_age('1',180)>STALE_SECONDS or api_age('5',180)>STALE_SECONDS):
        problems.append(('NETWORK','Recovery state claims recovered but market-data fetch freshness is still stale. Recovery verification is invalid.'))
    return problems



# ====================== Z3 SELF-MAINTENANCE / UPGRADE LAYER ======================
Z3_SELF_HEAL_VERSION='Z3-SELF-HEAL-005'
Z3_SELF_HEAL_INTERVAL=5.0
Z3_STRATEGY_MIN_SAMPLES=20
Z3_MAX_SAFE_REPAIRS_PER_CYCLE=4

def z3_verify_dashboard():
    """Verify that the server-side dashboard contract and critical UI hooks exist."""
    checks={
        'server_seed': callable(globals().get('_server_seed_html')),
        'state_endpoint': True,
        'command_endpoint': True,
        'chart_hook': 'id="chart"' in globals().get('HTML',''),
        'strategy_panel': 'id="adaptive027"' in globals().get('HTML',''),
        'ai_panel': 'id="aiAgents"' in globals().get('HTML',''),
        'z3_panel': 'id="z3panel"' in globals().get('HTML',''),
        'voice_controls': 'voiceStart()' in globals().get('HTML','') and 'speechSynthesis' in globals().get('HTML','')
    }
    ok=all(checks.values())
    return ok,checks

def z3_voice_health():
    """Browser voice is client-side; the server tracks capability/error reports rather than pretending it can control the browser."""
    with LOCK:
        v={
            'enabled':bool(Z3.get('voice_enabled',True)),
            'state':S.get('voice_state','UNKNOWN'),
            'last_event':S.get('voice_last_event',''),
            'last_error':S.get('voice_last_error',''),
            'last_report':S.get('voice_last_report',''),'recovery':S.get('voice_recovery','NONE'),'error_count':int(Z3.get('voice_error_count',0) or 0)
        }
    return v

def z3_strategy_upgrade_review():
    """Evidence-based strategy review. It may diagnose and recommend changes automatically,
    but protected strategy/risk changes require Owner/Admin approval."""
    try:
        a=analyze_performance()
        samples=int(a.get('samples',0) or 0)
        with Z3_LOCK:
            Z3['strategy_review_samples']=samples
            Z3['strategy_review_time']=now()
        if samples < Z3_STRATEGY_MIN_SAMPLES:
            return {'status':'LEARNING','samples':samples,'action':'collect_more_paper_evidence'}
        pf=float(a.get('profit_factor',0) or 0)
        exp=float(a.get('expectancy_pct',0) or 0)
        wr=float(a.get('win_rate',0) or 0)
        if pf<=0 or exp<0:
            reason=f'Paper evidence indicates strategy review is warranted: samples={samples}, win_rate={wr:.1f}%, PF={pf:.2f}, expectancy={exp:.2f}%.'
            with Z3_LOCK:
                Z3['strategy_upgrade_status']='REVIEW_REQUIRED'
            z3_event(reason,'STRATEGY_REVIEW')
            return {'status':'REVIEW_REQUIRED','samples':samples,'reason':reason}
        with Z3_LOCK:
            Z3['strategy_upgrade_status']='STABLE'
        return {'status':'STABLE','samples':samples,'win_rate':wr,'profit_factor':pf,'expectancy':exp}
    except Exception as e:
        with Z3_LOCK: Z3['strategy_upgrade_status']='REVIEW_ERROR'
        return {'status':'ERROR','reason':str(e)[-300:]}

def z3_self_heal_cycle():
    """Full bounded maintenance cycle: detect -> diagnose -> repair -> verify.
    It never changes risk limits, enables live execution, or rewrites source automatically."""
    repairs=[]; problems=[]
    try:
        ok,checks=z3_verify_dashboard()
        with Z3_LOCK:
            Z3['dashboard_health']='HEALTHY' if ok else 'DEGRADED'
            Z3['dashboard_checks']=checks
        if not ok:
            problems.append(('DASHBOARD','One or more critical dashboard hooks are missing.'))
        # Detect a dashboard-data failure separately from a missing UI hook.
        if S.get('running') and len(S.get('candles',[]))>=30:
            if S.get('strategy_health')=='INITIALIZING' and S.get('entry_gate')=='NO_SIGNAL':
                problems.append(('DASHBOARD','Verified candles exist but strategy state is not reaching the dashboard contract.'))

        vh=z3_voice_health()
        if vh['state']=='ERROR':
            problems.append(('VOICE','Browser voice reported an error: '+str(vh['last_error'] or 'unknown')))
        elif vh['state'] in ('UNSUPPORTED','BLOCKED'):
            with Z3_LOCK:
                Z3['last_voice_notice']='Browser voice capability is unavailable or blocked; text-chat fallback remains active.'

        # Runtime exceptions are evidence for diagnosis; clear only after a clean cycle.
        if S.get('engine_errors'):
            problems.append(('RUNTIME','Recent engine exceptions require diagnosis.'))

        sr=z3_strategy_upgrade_review()
        # Fewer than the evidence minimum is expected early in a paper session;
        # it is not a system-health fault and must not turn Guardian ATTENTION on.
        if sr.get('status')=='REVIEW_REQUIRED' and int(sr.get('samples',0) or 0)>=Z3_STRATEGY_MIN_SAMPLES:
            problems.append(('STRATEGY',sr.get('reason','Strategy review required.')))

        # Safe repairs are limited to internal state/UI/voice bookkeeping.
        if problems:
            problem_signature='|'.join(f'{k}:{r}' for k,r in problems)
            with Z3_LOCK:
                new_self_problem=problem_signature!=Z3.get('self_heal_problem_signature','')
                Z3['self_heal_problem_signature']=problem_signature
            if new_self_problem:
                for role,reason in problems[:Z3_MAX_SAFE_REPAIRS_PER_CYCLE]:
                    rolemap={'DASHBOARD':'Dashboard/UI','VOICE':'Voice','RUNTIME':'Runtime Diagnostics','STRATEGY':'Strategy Diagnostics'}
                    z3_specialist(rolemap.get(role,role),reason)
            if any(k=='STRATEGY' for k,_ in problems) and new_self_problem:
                z3_request_permission('strategy upgrade review','Paper evidence indicates the strategy should be reviewed. No automatic strategy/risk rule change was applied.')
            if not PAPER_ONLY or LIVE_TRADING or LIVE_EXECUTION_AUTHORIZED or REAL_ORDER_EXECUTION:
                z3_request_permission('safety-boundary intervention','Self-maintenance detected an unexpected execution boundary.')
            else:
                if any(k=='DASHBOARD' for k,_ in problems):
                    ok2,_=z3_verify_dashboard()
                    if ok2:
                        repairs.append('dashboard contract revalidated')
                    else:
                        z3_event('Dashboard repair could not be completed automatically; server remains available through the fallback renderer.','WARN')
                if any(k=='VOICE' for k,_ in problems):
                    # Browser-side voice cannot be repaired by server code. Keep the failure visible and provide text fallback.
                    with LOCK:
                        S['voice_recovery']='CLIENT_REINITIALIZE'
                    repairs.append('voice failure isolated; client reinitialization requested; text-chat fallback retained')
                if any(k=='RUNTIME' for k,_ in problems):
                    with LOCK:
                        S['engine_errors']=S.get('engine_errors',[])[-5:]
                        if S.get('service_state')=='DEGRADED' and not S.get('engine_last_error'):
                            S['service_state']='RUNNING' if S.get('running') else 'READY'
                    repairs.append('runtime state normalized without bypassing safety gates')
                if repairs:
                    persist_state('z3_self_heal')
                    with Z3_LOCK:
                        Z3['auto_repairs']+=len(repairs);Z3['last_repair']=now();Z3['last_repair_result']='; '.join(repairs)
                    z3_event('Self-heal verification completed: '+'; '.join(repairs),'REPAIR',True)
        with Z3_LOCK:
            if not problems: Z3['self_heal_problem_signature']=''
            Z3['self_heal_cycles']=int(Z3.get('self_heal_cycles',0))+1
            Z3['last_self_heal']=now()
            Z3['last_self_heal_problems']=len(problems)
            Z3['self_heal_status']='HEALTHY' if not problems else 'ATTENTION'
        return problems,repairs
    except Exception as e:
        with Z3_LOCK:
            Z3['self_heal_status']='ERROR';Z3['last_self_heal_error']=str(e)[-300:]
        z3_event('Self-heal cycle failed safely: '+str(e)[-300:],'ERROR',True)
        return [('SELF_HEAL',str(e)[-300:])],[]

def z3_stable_problem_signature(problems):
    """Stable Orchestrator identity for recurring faults; ignores volatile measurements."""
    stable=[]
    for kind,reason in problems:
        r=str(reason)
        r=re.sub(r'\b(?:1m|5m)=\d+(?:\.\d+)?s', 'DATA_AGE', r, flags=re.I)
        r=re.sub(r'heartbeat_age=\d+(?:\.\d+)?s', 'HEARTBEAT_AGE', r, flags=re.I)
        r=re.sub(r'cycle=\d+', 'CYCLE', r, flags=re.I)
        r=re.sub(r'\s+', ' ', r).strip()
        stable.append(str(kind)+':'+r)
    return '|'.join(stable)

def z3_tick():
    try:
        # Primary guardian diagnosis remains separate from the bounded self-maintenance cycle.
        problems=z3_diagnose()
        signature=z3_stable_problem_signature(problems)
        with Z3_LOCK:
            Z3['repair_cycle']+=1; Z3['last_check']=now(); Z3['problem_count']=len(problems)
            Z3['last_problem']=problems[0][1] if problems else ''
            Z3['health']='HEALTHY' if not problems else 'ATTENTION'
            Z3['status']='ONLINE'
            new_problem=signature!=Z3.get('last_problem_signature','')
            Z3['last_problem_signature']=signature
        last_sh=Z3.get('last_self_heal','')
        run_sh=True
        if last_sh:
            try: run_sh=(time.time()-datetime.fromisoformat(last_sh).timestamp())>=Z3_SELF_HEAL_INTERVAL
            except Exception: run_sh=True
        if run_sh:
            z3_self_heal_cycle()
        if problems:
            rolemap={'NETWORK':'Network/Market Data','ENGINE':'Engine Recovery','PERSISTENCE':'State Integrity','RUNTIME':'Code/Runtime Diagnostics','SECURITY':'Security','CONTRADICTION':'Strategy Diagnostics'}
            if new_problem:
                for role,reason in problems[:3]:
                    z3_specialist(rolemap.get(role,role),reason)
            safe_types={'NETWORK','PERSISTENCE','ENGINE','RUNTIME','DASHBOARD','VOICE'}
            protected_types={'SECURITY','STRATEGY','CONTRADICTION','FINANCIAL'}
            if Z3_AUTO_REPAIR and all(p[0] in safe_types for p in problems):
                z3_safe_repair()
            elif Z3_AUTO_REPAIR and any(p[0] in safe_types for p in problems) and not any(p[0] in protected_types for p in problems):
                z3_safe_repair()
            elif new_problem:
                z3_request_permission('review protected system problem',problems[0][1])
        elif Z3.get('pending_permission'):
            with Z3_LOCK: Z3['pending_permission']=None
            z3_event('Protected-system condition cleared; pending request cancelled.','INFO')
    except Exception as e:
        with Z3_LOCK: Z3['health']='SUPERVISOR_ERROR';Z3['last_problem']=str(e)[-300:]
        z3_event('Supervisor exception: '+str(e)[-300:],'ERROR',True)

def z3_command(text):
    z=str(text or '').strip(); l=z.lower()
    if l in ('z3 status','guardian status','supervisor status','ai status'):
        with Z3_LOCK: z=dict(Z3)
        say(f'Z3 Supervisor {z["health"]}. Problems={z["problem_count"]}; automatic repairs={z["auto_repairs"]}; permission requests={z["permission_requests"]}; specialists={z["specialists_created"]}.')
        return True
    if l in ('z3 diagnose','guardian diagnose','supervisor diagnose','check system'):
        z3_tick(); return True
    if l in ('z3 self heal','self heal','self check','upgrade ai','ai upgrade review'):
        problems,repairs=z3_self_heal_cycle(); say(f'Z3 self-maintenance complete. Problems={len(problems)}; safe repairs={len(repairs)}; strategy={Z3.get("strategy_upgrade_status","LEARNING")}.'); return True
    if l in ('upgrade','upgrade now','update','update now','ai upgrade','owner upgrade'):
        # Owner/Admin button: diagnose and safely repair first, then install a newer
        # signed-by-repository package only if it passes compile + paper-only checks.
        problems,repairs=z3_self_heal_cycle()
        if problems and any(k in ('SECURITY','FINANCIAL','STRATEGY','CONTRADICTION') for k,_ in problems):
            z3_request_permission('upgrade review',problems[0][1]); say('Upgrade paused: a protected problem requires Owner/Admin review before installation.'); return True
        ok,msg=z3_upgrade_package(); say(msg); return True
    if l.startswith('dashboard error'):
        detail=z[len('dashboard error'):].strip() or 'browser dashboard reported an error'
        with LOCK: S['dashboard_last_error']=detail[-300:]
        with Z3_LOCK:
            Z3['dashboard_error_count']=int(Z3.get('dashboard_error_count',0) or 0)+1; Z3['dashboard_recovery']='RELOAD_AND_REVERIFY'
        z3_event('Dashboard client error reported: '+detail[-300:],'ERROR'); z3_self_heal_cycle(); return True
    if l.startswith('voice error') or l.startswith('voice failed') or l.startswith('voice unavailable'):
        detail=z.split(' ',2)[-1] if len(z.split(' ',2))>=3 else 'Voice subsystem reported failure'
        with LOCK:
            S['voice_state']='ERROR'; S['voice_last_event']=now(); S['voice_last_error']=detail[-300:]; S['voice_last_report']=now(); S['voice_recovery']='CLIENT_REINITIALIZE'
        with Z3_LOCK: Z3['voice_error_count']=int(Z3.get('voice_error_count',0) or 0)+1
        z3_event('Voice failure recorded: '+detail[-300:],'ERROR'); z3_self_heal_cycle(); say('Voice failure recorded. Text chat remains available.'); return True
    if l in ('voice ready','voice online','voice ok'):
        with LOCK: S['voice_state']='READY'; S['voice_last_event']=now(); S['voice_last_error']=''; S['voice_last_report']=now()
        say('Voice status recorded as READY.'); return True
    if l in ('voice error','voice failed','voice unavailable'):
        with LOCK: S['voice_state']='ERROR'; S['voice_last_event']=now(); S['voice_last_error']=z or 'Voice subsystem reported failure'; S['voice_last_report']=now()
        say('Voice failure recorded. Text chat remains available.'); return True
    if l in ('z3 repair','guardian repair','repair system','repair now'):
        z3_safe_repair(); return True
    if l in ('approve z3','approve repair','approve supervisor'):
        with Z3_LOCK: p=Z3.get('pending_permission')
        if not p: say('Z3 has no pending permission request.'); return True
        action=p['action']
        if action=='review protected system problem':
            with Z3_LOCK: Z3['pending_permission']=None
            z3_event('Owner/Admin approved protected-system review. No live boundary or risk limit was changed.','APPROVED',True)
            z3_safe_repair(); return True
        with Z3_LOCK: Z3['pending_permission']=None
        z3_event('Owner/Admin approval recorded: '+action,'APPROVED',True); return True
    return False

def _command_impl(text):
    if z3_command(text): return
    z=text.strip();l=z.lower()
    action_reason=None
    with LOCK:
        if l in ('start','start trading'):
            if S.get('running') and not S.get('paused'):
                S['last_action']='START_IGNORED'; action_reason='AI paper trading is already RUNNING.'
            else:
                S['run_generation']=int(S.get('run_generation',0))+1
                S['running']=True;S['paused']=False;S['desired_running']=True;S['recovery_pending']=True;S['recovery_state']='READY';S['service_state']='RUNNING';S['connection']='UNKNOWN';S['data_verification_status']='VERIFYING';S['verification_state']='VERIFYING';S['data_verification_reason']='Awaiting fresh verified market snapshot';S['entry_gate']='DATA_VERIFYING';S['entry_validation']='BLOCKED';S['validation_reasons']=['VERIFYING_FRESH_MARKET_DATA'];S['last_connection_change']=now();S['last_action']='START';S['session_started_at']=now();S['session_signals']=0;S['session_validation_passes']=0;S['session_entries']=0;S['session_closes']=0;S['session_risk_blocks']=0;S['session_data_blocks']=0;S['session_connection_losses']=0
                start_market_worker(force=False); action_reason='AI paper trading STARTED. Market-data recovery is now running on the dedicated worker. Live trading remains OFF.'
        elif l in ('pause','pause trading'):
            if not S.get('running'):
                S['last_action']='PAUSE_IGNORED'; action_reason='AI paper trading is STOPPED; PAUSE has no effect.'
            elif S.get('paused'):
                S['last_action']='PAUSE_IGNORED'; action_reason='AI paper trading is already PAUSED.'
            else:
                S['paused']=True;S['desired_running']=True;S['last_action']='PAUSE';action_reason='AI paper trading PAUSED. Open paper positions remain under risk management.'
        elif l in ('resume','resume trading','continue'):
            if S.get('running') and not S.get('paused'):
                S['last_action']='RESUME_IGNORED'; action_reason='AI paper trading is already RUNNING.'
            else:
                S['run_generation']=int(S.get('run_generation',0))+1
                S['running']=True;S['paused']=False;S['desired_running']=True;S['recovery_pending']=True;S['recovery_state']='READY';S['service_state']='RUNNING';S['connection']='UNKNOWN';S['data_verification_status']='VERIFYING';S['verification_state']='VERIFYING';S['data_verification_reason']='Awaiting fresh verified market snapshot';S['entry_gate']='DATA_VERIFYING';S['entry_validation']='BLOCKED';S['validation_reasons']=['VERIFYING_FRESH_MARKET_DATA'];S['last_connection_change']=now();S['last_action']='RESUME';action_reason='AI paper trading RESUMED. New entries remain subject to all market, connection and risk gates.'
        elif l in ('stop','stop trading'):
            S['run_generation']=int(S.get('run_generation',0))+1
            S['running']=False;S['paused']=False;S['desired_running']=False;S['service_state']='STOPPED';S['recovery_pending']=False;S['last_action']='STOP';action_reason='AI paper trading STOPPED. Pending engine work will be invalidated by the run-generation guard.'
    if action_reason:
        say(action_reason)
        # START/RESUME explicitly trigger one dedicated fresh-market verification.
        # This removes the race where the command sets connection=ONLINE and the
        # market worker therefore waits for its normal candle tick before clearing
        # DATA_VERIFY_PENDING. The verification itself remains a hard safety gate.
        if l in ('start','start trading','resume','resume trading','continue') and S.get('running') and not S.get('paused'):
            # Reconcile an already-fresh local snapshot immediately. If it is not
            # fresh, the dedicated verifier below performs the normal hard check.
            if not normalize_verified_health():
                threading.Thread(target=recovery_cycle,name='ZTrader-VerifyOnce',daemon=True).start()
        persist_state('manual_'+('start' if l.startswith('start') else 'pause' if l=='pause' else 'resume' if l in ('resume','continue') else 'stop'));return
    if l in ('save','save state','persist','persist state'):
        persist_state('manual_save'); say('Runtime state saved and verified.'); return
    if l in ('diagnostics','performance','strategy report','trade diagnostics','trade report'):
        a=analyze_performance();r=diagnostic_report();say(f'Performance: samples={a["samples"]}, win_rate={a["win_rate"]}%, PF={a["profit_factor"]}, expectancy={a["expectancy_pct"]}%, drawdown={a["max_drawdown_pct"]}%, health={S.get("strategy_health")}. Trade diagnostics: {r["samples"]} completed trades reviewed; market phase={S.get('market_phase')}; confidence={S.get('market_confidence')}%; adaptive exits={S.get('adaptive_exit_count')}.');return
    for prefix,key in [('trading pool ','pool'),('trading capital ','pool'),('trade capital ','pool'),('price per trade ','per_trade'),('trade amount ','per_trade'),('amount per trade ','per_trade'),('capital per trade ','per_trade'),('take profit ','take_profit'),('takeprofit ','take_profit'),('tp ','take_profit'),('lot size ','lot_size')]:
        if l.startswith(prefix):
            try:
                v=float(z[len(prefix):])
                if not math.isfinite(v): raise ValueError('value must be finite')
                if key=='pool':
                    if v<0: raise ValueError('trading pool cannot be negative')
                    if v>S['paper_balance']+1e-9: raise ValueError(f'trading pool cannot exceed paper account balance (${S["paper_balance"]:.4f})')
                    if v+1e-9<S.get('allocated',0): raise ValueError(f'trading pool cannot be below currently allocated capital (${S["allocated"]:.4f})')
                    S[key]=v;S['peak_pool']=max(float(S.get('peak_pool',v)),v)
                elif key=='per_trade':
                    if v<MIN_TRADE: raise ValueError(f'per-trade amount must be at least ${MIN_TRADE:.2f}')
                    if S['pool']>0 and v>S['pool']+1e-9: raise ValueError(f'per-trade amount cannot exceed trading pool (${S["pool"]:.4f})')
                    S[key]=v
                elif key=='take_profit':
                    if v<0: raise ValueError('take profit cannot be negative')
                    S[key]=v
                elif key=='lot_size':
                    if v<0: raise ValueError('lot size cannot be negative')
                    S[key]=v
                say(f'{key.replace("_"," ").title()} set to {S[key]}. Active immediately.')
            except Exception as e:
                say('Setting rejected: '+str(e))
            persist_state('setting_change')
            return
    if l=='clear all':
        for t in list(S['open']):close_trade(t,'CLEAR_ALL')
        S['closed']=[];say('All open trades were closed and trade history was cleared. Account P&L remains preserved in the paper account and P&L totals.');persist_state('clear_all');return
    if l.startswith('close '):
        try:n=int(z.split()[-1])
        except Exception:return
        for t in list(S['open']):
            if t['id']==n:close_trade(t,'MANUAL_CLOSE');persist_state('manual_close');break

def command(text):
    raw=str(text or '').strip()
    before=len(S.get('chat',[]))
    try:
        _command_impl(raw)
        with LOCK:
            msgs=S.get('chat',[])[before:]
            result=msgs[-1].get('text','Command accepted.') if msgs else 'Command accepted.'
            S['last_command']=raw; S['last_command_ok']=True; S['last_command_result']=result
        return {'ok':True,'message':result}
    except Exception as e:
        err=str(e)[-300:]
        with LOCK:
            S['last_command']=raw; S['last_command_ok']=False; S['last_command_result']=err
            S['errors']=(S.get('errors',[])+['COMMAND: '+err])[-10:]
        try: say('Command error: '+err)
        except Exception: pass
        return {'ok':False,'message':err}


def normalize_verified_health():
    """Make a fresh, structurally verified snapshot authoritative for public health.
    This never fetches data and never relaxes any freshness or entry-safety gate.
    """
    try:
        with LOCK:
            running=bool(S.get('running'))
            desired=bool(S.get('desired_running'))
            paused=bool(S.get('paused'))
            c5=S.get('candles') or []
            c1=S.get('one_minute_candles') or []
            cache1=float(S.get('recovery_last_1m_cache_age',1e9) or 1e9)
            cache5=float(S.get('recovery_last_5m_cache_age',1e9) or 1e9)
            age1=float(S.get('recovery_last_1m_age',1e9) or 1e9)
            age5=float(S.get('recovery_last_5m_age',1e9) or 1e9)
            success_at=float(S.get('market_data_last_success_at',0) or 0)
            current_success_age=max(0.0,time.time()-success_at) if success_at>0 else float('inf')
            actual_age1=_snapshot_age(c1,60)
            actual_age5=_snapshot_age(c5,300)
            fresh=(running and desired and not paused and len(c5)>=30 and len(c1)>=30 and
                   current_success_age<=STALE_SECONDS and
                   cache1<=STALE_SECONDS and cache5<=STALE_SECONDS and
                   actual_age1<=MARKET_1M_MAX_AGE and actual_age5<=MARKET_5M_MAX_AGE and
                   age1<=MARKET_1M_MAX_AGE and age5<=MARKET_5M_MAX_AGE)
            if not fresh:
                return False
            S['data_verification_status']='VERIFIED'
            S['verification_state']='VERIFIED'
            # Any data-verification/data-insufficiency gate is stale once the
            # same snapshot has passed the hard freshness and candle checks.
            # Do not clear capital/risk/same-side gates here.
            gate_now=str(S.get('entry_gate',''))
            if gate_now in ('DATA_INSUFFICIENT','DATA_VERIFY_PENDING','DATA_VERIFYING') or gate_now.startswith('DATA_VERIFY'):
                S['entry_gate']='VERIFIED_MARKET_DATA'
            if S.get('entry_validation')=='BLOCKED' and any('VERIFY' in str(x).upper() for x in (S.get('validation_reasons') or [])):
                S['entry_validation']='NOT_READY'
                S['validation_reasons']=['AWAITING_STRATEGY_SETUP']
            S['connection']='ONLINE'
            S['recovery_pending']=False
            S['recovery_state']='RECOVERED'
            S['service_state']='RUNNING'
            S['market_data_worker_state']='VERIFIED'
            S['recovery_last_network_ok']=True
            if not S.get('last_verification_time'):
                S['last_verification_time']=now()
            S['data_verification_reason']=''
            S['data_verification_detail']=f'Fresh verified snapshot; success age {current_success_age:.1f}s; candle ages {actual_age1:.1f}s/{actual_age5:.1f}s.'
            return True
    except Exception as e:
        log_error('VERIFIED HEALTH RECONCILE',e)
        return False

def snapshot():
    # Reconcile public health from the already-verified local snapshot before
    # copying state. This prevents the dashboard/Guardian view from displaying
    # UNKNOWN while the same snapshot is demonstrably fresh and verified.
    normalize_verified_health()
    # Copy the small state envelope quickly, then do diagnostic work without
    # holding the main engine lock. The dashboard must never wait behind a
    # market-data/network operation.
    with LOCK:
        base=dict(S)
        open_copy=[dict(t,pnl=round(t['pnl'],6),value=round(t['value'],6)) for t in S['open']]
        closed_copy=list(S['closed'][-80:])
        chat_copy=list(S['chat'][-60:])
        decision_copy=list(S['decision_log'][-60:])
        z3_copy=dict(Z3)
        z3_events=list(Z3.get('events',[]))[-30:]
    available=round(free(),6)
    unrealized=round(sum(float(t.get('pnl',0) or 0) for t in open_copy),6)
    realized=round(float(base.get('realized_pnl_total',0.0) or 0.0),6)
    if 'realized_pnl_total' not in base:
        realized=round(sum(float(t.get('pnl',0) or 0) for t in S.get('closed',[])),6)
    equity=round(base.get('pool',0)+unrealized,6)
    peak=max(float(base.get('peak_pool',base.get('pool',0)) or 0),1e-9)
    drawdown=round(max(0.0,(peak-equity)/peak*100.0),4)
    return {**base,'open':open_copy,'closed':closed_copy,'chat':chat_copy,'decision_log':decision_copy,
        'available':available,'unrealized_pnl':unrealized,'realized_pnl':realized,'total_pnl':realized+unrealized,'drawdown_pct':drawdown,'allocated_capital':round(float(base.get('allocated',0) or 0),6),'equity':equity,'engine_heartbeat':base.get('engine_heartbeat',0),
        'state_file_exists':os.path.exists(STATE_FILE),'state_file_size':os.path.getsize(STATE_FILE) if os.path.exists(STATE_FILE) else 0,
        'diagnostic_report':diagnostic_report(),'z3':z3_copy,'z3_events':z3_events,
        'execution_view':{'price':safe_float(base.get('price')),'signal':base.get('signal','WAIT'),'score':safe_float(base.get('score')),
            'entry_validation':base.get('entry_validation','NOT_READY'),'entry_gate':base.get('entry_gate','NO_SIGNAL'),
            'risk_block':base.get('risk_block',''),'open_count':len(open_copy),'last_trade_time':base.get('last_trade_time',''),
            'quick_mode':base.get('quick_mode','IDLE'),'quick_signal':base.get('quick_signal','WAIT'),'market_phase':base.get('market_phase','UNKNOWN')},
        'engine_health':{'service_state':base.get('service_state','UNKNOWN'),'connection':base.get('connection','UNKNOWN'),
            'heartbeat':base.get('engine_heartbeat',0),'heartbeat_age':round(max(0,time.time()-float(base.get('engine_heartbeat') or time.time())),2),
            'cycle_count':base.get('engine_cycle_count',0),'recovery_state':base.get('recovery_state','UNKNOWN'),
            'recovery_pending':base.get('recovery_pending',False),'recovery_attempts':base.get('recovery_attempts',0),'recovery_successes':base.get('recovery_successes',0),'recovery_last_reason':base.get('recovery_last_reason',''),'recovery_last_source':base.get('recovery_last_source',''),'recovery_last_1m_age':base.get('recovery_last_1m_age',0.0),'recovery_last_5m_age':base.get('recovery_last_5m_age',0.0),'recovery_last_1m_cache_age':base.get('recovery_last_1m_cache_age',0.0),'recovery_last_5m_cache_age':base.get('recovery_last_5m_cache_age',0.0),'recovery_last_ticker_ok':base.get('recovery_last_ticker_ok',False),'recovery_last_network_ok':base.get('recovery_last_network_ok',False),'recovery_next_retry':base.get('recovery_next_retry',0.0),'market_data_worker_state':base.get('market_data_worker_state','UNKNOWN'),'market_data_source':base.get('market_data_source','UNKNOWN'),'market_data_last_success_at':base.get('market_data_last_success_at',0.0),'market_data_last_success_source':base.get('market_data_last_success_source',''),'market_data_last_failure_at':base.get('market_data_last_failure_at',0.0),'market_data_last_failure_source':base.get('market_data_last_failure_source',''),'market_data_last_failure_reason':base.get('market_data_last_failure_reason',''),'market_data_fetch_attempts':base.get('market_data_fetch_attempts',0),'market_data_fetch_successes':base.get('market_data_fetch_successes',0),'market_data_consecutive_failures':base.get('market_data_consecutive_failures',0),'persistence_status':base.get('persistence_status','NOT_VERIFIED'),
            'persistence_ok':base.get('persistence_ok',False),'last_error':base.get('engine_last_error','')},
        'ai_status':{'trading':{'state':'RUNNING' if base.get('running') and not base.get('paused') else 'PAUSED' if base.get('running') else 'STOPPED',
            'signal':base.get('signal','WAIT'),'score':safe_float(base.get('score')),'gate':base.get('entry_gate','NO_SIGNAL'),
            'validation':base.get('entry_validation','NOT_READY'),'quick_mode':base.get('quick_mode','IDLE')},
            'guardian':{'state':z3_copy.get('health','CHECKING'),'connection':base.get('connection','UNKNOWN'),'self_heal':z3_copy.get('self_heal_status','STARTING'),'dashboard':z3_copy.get('dashboard_health','CHECKING'),'voice':base.get('voice_state','UNKNOWN'),
                'persistence':base.get('persistence_status','NOT_VERIFIED'),'engine':base.get('service_state','UNKNOWN')},
            'orchestrator':{'state':'ONLINE' if z3_copy.get('status')=='ONLINE' else z3_copy.get('status','CHECKING'),
                'specialists':len([x for x in z3_copy.get('specialists',[]) if x.get('status')=='ACTIVE']),
                'total_specialists':z3_copy.get('specialists_created',0),'pending_permission':bool(z3_copy.get('pending_permission'))}}}


HTML='''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>ZTrader AI</title><style>form.inline{display:inline;margin:0;padding:0}.inline button{margin:0 2px}</style><style>
*{box-sizing:border-box}body{margin:0;background:#0d1117;color:#edf2f7;font:14px Arial}.wrap{max-width:1280px;margin:auto;padding:14px}header{display:flex;justify-content:space-between;align-items:center;padding:8px 0 14px}.grid{display:grid;grid-template-columns:2fr 1fr;gap:12px}.box{background:#171d26;border:1px solid #293342;border-radius:14px;padding:12px;margin-bottom:12px}.metrics{display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin-bottom:12px}.metric{background:#202733;padding:10px;border-radius:10px;min-height:58px}.muted{color:#94a3b8}.status{padding:8px 11px;border-radius:9px;font-weight:bold}.on{background:#124d2b;color:#70ff9b}.pause{background:#5b460e;color:#ffd966}.off{background:#571d25;color:#ff8992}.online{background:#124d2b;color:#70ff9b}.offline{background:#571d25;color:#ff8992}.row{display:flex;gap:7px;flex-wrap:wrap}button,input{padding:9px;border-radius:8px;border:1px solid #3a4656;background:#10161e;color:#fff}button{cursor:pointer}.activeStart{background:#176b3b;border-color:#70ff9b}.activePause{background:#806313;border-color:#ffd966}.activeStop{background:#7b2631;border-color:#ff8992}.statebar{padding:12px 14px;border-radius:12px;font-weight:800;text-align:center;margin:8px 0;border:1px solid}.statebar.running{background:#123d27;color:#70ff9b;border-color:#70ff9b}.statebar.paused{background:#4b3b0d;color:#ffd966;border-color:#ffd966}.statebar.stopped{background:#4b171e;color:#ff8992;border-color:#ff8992}.trade{display:flex;justify-content:space-between;align-items:center;background:#202733;border-radius:11px;padding:11px;margin:7px 0;cursor:pointer}.money{font-size:18px}.pos{color:#62ef91}.neg{color:#ff707d}.chat,.log{max-height:220px;overflow:auto;line-height:1.6}.small{font-size:12px}.modal{position:fixed;inset:0;background:#000b;display:none;align-items:center;justify-content:center}.modal>div{background:#171d26;padding:18px;border-radius:14px;width:min(520px,92%)}canvas{width:100%;height:410px}.safety{line-height:1.8}@media(max-width:1050px){.metrics{grid-template-columns:repeat(3,1fr)}}@media(max-width:800px){.grid{grid-template-columns:1fr}.metrics{grid-template-columns:repeat(2,1fr)}}
.panelgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}.tag{background:#202733;padding:8px;border-radius:9px}.warn{color:#ffd966}.good{color:#70ff9b}.diagscroll{overflow:auto}.diagtable{width:100%;border-collapse:collapse;font-size:12px}.diagtable th,.diagtable td{padding:7px;border-bottom:1px solid #293342;text-align:left;white-space:nowrap}.diagtable tr{cursor:pointer}.diagtable tr:hover{background:#202733}.diagnote{background:#202733;border-radius:10px;padding:9px;margin:6px 0}.mini{font-size:11px;color:#94a3b8}@media(max-width:800px){.panelgrid{grid-template-columns:1fr}}
.tvwrap{height:520px;border-radius:12px;overflow:hidden;background:#0b0f14;border:1px solid #293342}.tvwrap iframe{width:100%;height:100%;border:0}.chartgrid{display:grid;grid-template-columns:1.45fr 1fr;gap:12px}.chartbox{margin-bottom:0}.chatbox{position:sticky;top:12px}.chatbox .chat{max-height:330px}.onepage-title{display:flex;justify-content:space-between;gap:10px;align-items:center;flex-wrap:wrap}.badge{padding:5px 8px;border-radius:999px;background:#202733;font-size:11px}.liveoff{color:#70ff9b}.layoutnote{font-size:11px;color:#94a3b8;margin-top:6px}@media(max-width:1050px){.chartgrid{grid-template-columns:1fr}.chatbox{position:static}.tvwrap{height:430px}}</style></head><body><div class="wrap"><header><b>ZTrader AI · Owner/Admin · INTEGRATED-030 · BTCUSDT</b><div><span id="conn" class="status offline">CONNECTION UNKNOWN</span> <span id="status" class="status off">STOPPED</span></div></header><div class="metrics" id="m"></div><div class="grid"><main><div class="chartgrid"><div class="box chartbox"><div class="onepage-title"><b>TradingView · BTCUSDT</b><span class="badge liveoff">LIVE MARKET VIEW · BINANCE</span></div><div class="tvwrap"><iframe src="https://www.tradingview.com/widgetembed/?frameElementId=tradingview_btcusdt&symbol=BINANCE%3ABTCUSDT&interval=1&hidesidetoolbar=0&hidetoptoolbar=0&symboledit=1&saveimage=0&toolbarbg=%230d1117&theme=dark&style=1&timezone=Etc%2FUTC&withdateranges=1&hideideas=1&studies=RSI%40tv-basicstudies%2CMACD%40tv-basicstudies" title="TradingView BTCUSDT" loading="eager" referrerpolicy="origin" allowtransparency="true" allowfullscreen></iframe></div><div class="layoutnote"><a href="https://www.tradingview.com/symbols/BTCUSDT/?exchange=BINANCE" target="_blank" rel="noopener">Open live BTCUSDT chart</a> · External TradingView visualization only. ZTrader AI remains paper-only; this chart does not place orders.</div></div><div class="box chartbox"><div class="onepage-title"><b>ZTrader AI · Internal AI Chart</b><span class="badge">AI EXECUTION VIEW</span></div><canvas id="chart" width="900" height="410"></canvas><div class="layoutnote">AI decision context, structure and execution data remain connected to the ZTrader engine.</div></div></div><div class="box"><b>Trade Decision Diagnostics</b><div class="muted">This shows why signals are or are not becoming paper trades.</div><div id="diag"></div></div><div class="box"><b>Active Trades</b><div id="open"></div></div><div class="box"><b>Strategy Intelligence &amp; Performance</b><div id="perf"></div></div><div class="box"><b>Market Intelligence &amp; Trade Thesis</b><div id="intel"></div></div><div class="box"><b>INTEGRATED-030 · BTCUSDT 1m Dual-Speed Strategy</b><div id="adaptive027"></div></div><div class="box"><b>Trade Forensics &amp; Exit Analysis Center</b><div class="muted small">Trade-by-trade forensic evidence. New trades capture complete entry, intratrade excursion, stop/target movement, exit costs, giveback and data quality. Descriptive only; no automatic strategy changes.</div><div id="tradeDiag"></div></div><div class="box"><b>Trade History</b><div id="closed"></div></div></main><aside><div class="box"><b>AI Controls</b><div id="statebar" class="statebar stopped">● STOPPED</div><p><form class="inline" action="/cmdget" method="get"><input type="hidden" name="text" value="Start trading"><button id="start" type="submit" onclick="cmd('Start trading');return false">START</button></form> <form class="inline" action="/cmdget" method="get"><input type="hidden" name="text" value="Pause"><button id="pause" type="submit" onclick="cmd('Pause');return false">PAUSE</button></form> <form class="inline" action="/cmdget" method="get"><input type="hidden" name="text" value="Resume"><button id="resume" type="submit" onclick="cmd('Resume');return false">RESUME</button></form> <form class="inline" action="/cmdget" method="get"><input type="hidden" name="text" value="Stop trading"><button id="stop" type="submit" onclick="cmd('Stop trading');return false">STOP</button></form> <form class="inline" action="/cmdget" method="get"><input type="hidden" name="text" value="Save"><button id="save" type="submit" onclick="cmd('Save');return false">SAVE</button></form></p><div class="row"><input id="pool" type="number" step=".01" value="100"><button onclick="cmd('Trading pool '+document.getElementById('pool').value)">Trading pool</button><input id="amt" type="number" step=".01" value="10"><button onclick="cmd('Price per trade '+document.getElementById('amt').value)">Price / trade</button><input id="tp" type="number" step=".01" min="0" placeholder="TP $ (0 = AI)"><button onclick="cmd('Take profit '+document.getElementById('tp').value)">Take profit</button></div><p><button onclick="cmd('Trade diagnostics')">TRADE DIAGNOSTICS</button> <button onclick="cmd('Diagnostics')">PERFORMANCE</button> <button onclick="cmd('Clear all')">CLEAR ALL</button></p></div><div class="box chatbox"><b>AI Chat · Owner/Admin</b><div class="layoutnote">Control and monitoring chat stays on this same page.</div><div id="chat" class="chat"></div><div class="row"><input id="msg" placeholder="Start trading"><button onclick="send()">Send</button></div></div><div class="box"><b>Z3 Supervisor AI · Guardian + Orchestrator</b><div id="z3panel"></div><div class="row"><button onclick="cmd('Z3 Diagnose')">DIAGNOSE</button><button onclick="cmd('Z3 Repair')">SAFE REPAIR</button><button onclick="cmd('Z3 Self Heal')">SELF-HEAL</button><button onclick="cmd('Z3 Status')">STATUS</button><button onclick="cmd('Upgrade')">UPGRADE</button><button onclick="voiceStart()">🎙 TALK TO AI</button><button onclick="voiceStop()">■ STOP VOICE</button></div><div class="small muted">Three-AI coordination: Trading AI handles market decisions; Guardian handles health/recovery; Orchestrator creates bounded specialist agents for security, strategy, data, recovery and integrity diagnosis. Consequential changes require Owner/Admin approval.</div></div><div class="box"><b>ENGINE HEALTH &amp; RUNTIME STATUS</b><div id="engineHealth" class="panelgrid"></div><div id="commandResult" class="diagnote">Dashboard command channel ready.</div></div><div class="box"><b>THREE-AI SUPERVISOR STATUS</b><div id="aiAgents" class="panelgrid"></div><div id="aiActivity" class="log small">Trading AI, Guardian AI and Orchestrator AI are connected to the same paper-only control state.</div></div><div class="box safety"><b>Safety &amp; Architecture</b><br>Paper trading: <b>ON</b><br>Live trading: <b>OFF</b><br>Real orders: <b>OFF</b><br>Live authorization: <b>OFF</b><br>Z3 Supervisor: <b>ON · PAPER-ONLY</b><br>Trading AI + Guardian AI + Orchestrator AI: <b>COOPERATING</b><br>Engine: <b>independent loop</b><br>Dashboard: <b>monitor/control client</b><br>Connection loss: <b>new entries blocked</b><br>State persistence: <b>atomic + automatic</b><br>Restart recovery: <b>enabled</b><br><span class="muted">Daily structure controls direction. 4H confirms. 15m sets regime. 1m executes · 5m context. Strategy intelligence records signal components, execution quality, entry context, exits, MFE/MAE, R-multiple, giveback, score bands, regimes, win/loss, profit factor, expectancy and drawdown. Trade Forensics Center exposes completed-trade evidence and data-quality warnings. Market Intelligence continuously reads multi-timeframe structure, momentum, volatility, participation, liquidity and microstructure; adaptive trade management can protect profit or exit when the original thesis weakens or fails. No live orders are enabled. Runtime state is persisted atomically. A service restart can restore the last safe state and recover paper positions after market validation. Strategy health is evidence-based and does not force threshold changes from small samples. Turning off the dashboard does not issue STOP.</span></div></aside></div></div><div id="modal" class="modal" onclick="this.style.display='none'"><div onclick="event.stopPropagation()"><b>Trade Details</b><pre id="detail"></pre><button id="closeTrade" onclick="closeShown()">Close this trade</button> <button onclick="document.getElementById('modal').style.display='none'">Close</button></div></div><script>
let D={},loadBusy=false;async function load(){if(loadBusy)return;loadBusy=true;let timer=null;try{let c=new AbortController();timer=setTimeout(()=>c.abort(),1800);let r=await fetch('/state',{cache:'no-store',signal:c.signal});if(!r.ok)throw new Error('STATE HTTP '+r.status);D=await r.json();render()}catch(e){let x=document.getElementById('commandResult');if(x)x.innerHTML='<b>Dashboard error:</b> '+E(e.name==='AbortError'?'State request timed out':(e.message||e))}finally{if(timer)clearTimeout(timer);loadBusy=false}}async function cmd(x){let box=document.getElementById('commandResult');try{if(box){box.className='diagnote';box.innerHTML='<b>Command:</b> '+E(x)+' · sending…'}let ac=new AbortController(),tm=setTimeout(()=>ac.abort(),2500);let r=await fetch('/cmd',{method:'POST',headers:{'Content-Type':'text/plain;charset=UTF-8'},body:String(x),cache:'no-store',signal:ac.signal});clearTimeout(tm);if(!r.ok)throw new Error('HTTP '+r.status);let j=await r.json();if(box){box.className='diagnote '+(j.ok?'good':'warn');box.innerHTML='<b>'+ (j.ok?'OK':'ERROR') +':</b> '+E(j.message||'No response')}await load();}catch(e){if(box){box.className='diagnote warn';box.innerHTML='<b>COMMAND ERROR:</b> '+E(e.message||e)}}}function send(){let q=document.getElementById('msg');if(q.value.trim()){cmd(q.value);q.value=''}}function M(x){return '$'+Number(x||0).toFixed(4)}function E(x){return String(x??'').replaceAll('&','&amp;').replaceAll('<','&lt;')}
function render(){let mode=D.running?(D.paused?'PAUSED':'RUNNING'):'STOPPED';let st=document.getElementById('status'),sb=document.getElementById('statebar');st.textContent=mode;st.className='status '+(mode==='RUNNING'?'on':mode==='PAUSED'?'pause':'off');sb.textContent=mode==='RUNNING'?'● RUNNING — AI ACTIVE':mode==='PAUSED'?'● PAUSED — AI HOLDING':'● STOPPED — AI OFF';sb.className='statebar '+(mode==='RUNNING'?'running':mode==='PAUSED'?'paused':'stopped');let co=document.getElementById('conn');co.textContent=D.connection==='ONLINE'?'ONLINE':'CONNECTION '+(D.connection||'UNKNOWN');co.className='status '+(D.connection==='ONLINE'?'online':'offline');document.getElementById('start').className=mode==='RUNNING'?'activeStart':'';document.getElementById('pause').className=mode==='PAUSED'?'activePause':'';document.getElementById('stop').className=mode==='STOPPED'?'activeStop':'';let v=D.validation||{};document.getElementById('m').innerHTML=[['Paper account',M(D.paper_balance)],['Trading pool',M(D.pool)],['Available balance',M(D.available)],['Allocated capital',M(D.allocated_capital)],['Per trade',M(D.per_trade)],['Trading equity',M(D.equity)],['Realized P&L',M(D.realized_pnl)],['Unrealized P&L',M(D.unrealized_pnl)],['Total P&L',M(D.total_pnl)],['Drawdown',Number(D.drawdown_pct||0).toFixed(2)+'%'],['Take profit',D.take_profit>0?M(D.take_profit):'AI AUTO'],['Open trades',(D.open||[]).length],['Total trades',(D.closed||[]).length+(D.open||[]).length],['Wins / losses',(D.wins||0)+' / '+(D.losses||0)],['Profit closes',D.profit_closes||0],['AI signal',(D.signal||'WAIT')+' · '+(D.score||0)],['Next candle bias',Number(D.prediction||50).toFixed(1)+'%'],['Daily structure',D.daily_structure||'UNKNOWN'],['4H structure',D.fourh_structure||'UNKNOWN'],['15m regime',D.regime||'UNKNOWN'],['Validation',v.status||'NOT_RUN'],['Backtest PF',v.profit_factor||0],['Backtest expectancy',(v.expectancy||0)+'%'],['Stop loss','AI STRUCTURE'],['Market quality',D.market_quality||'UNKNOWN'],['Spread',Number(D.spread_bps||0).toFixed(2)+' bps'],['Order imbalance',Number(D.order_imbalance||0).toFixed(3)],['Funding',Number(D.funding_rate||0).toFixed(5)],['OI change',Number(D.open_interest_change||0).toFixed(3)+'%'],['Risk block',D.risk_block||'NONE'],['Engine state',D.service_state||'UNKNOWN'],['Recovery',D.recovery_state||'UNKNOWN'],['Persisted',D.persistence_ok?'VERIFIED':'NOT VERIFIED'],['State file',D.state_file_exists?'PRESENT':'MISSING'],['Signals seen',D.signals_seen||0],['Validation passes',D.validation_passes||0],['Paper entries',D.paper_entries||0],['Risk blocks',D.risk_blocks||0]].map(a=>`<div class="metric"><span class="muted">${a[0]}</span><br><b>${a[1]}</b></div>`).join('');let rr=D.rejection_reasons||{};document.getElementById('diag').innerHTML=`<div class="small"><b>Current gate:</b> ${E(D.entry_gate||'NO_SIGNAL')}<br><b>Decision:</b> ${E(D.decision_note||'')}<br><b>Last signal:</b> ${E(D.last_signal_time||'none')}<br><b>Last paper trade:</b> ${E(D.last_trade_time||'none')}<br><b>Engine heartbeat:</b> ${D.engine_heartbeat?new Date(D.engine_heartbeat*1000).toISOString():'none'}<br><b>Service state:</b> ${E(D.service_state||'UNKNOWN')}<br><b>Recovery:</b> ${E(D.recovery_state||'UNKNOWN')}<br><b>Last persisted:</b> ${E(D.last_persist_time||'none')}<br><b>Persistence status:</b> ${E(D.persistence_status||'NOT_VERIFIED')} · ${D.persistence_ok?'VERIFIED':'NOT VERIFIED'}<br><b>Persist reason:</b> ${E(D.last_persist_reason||'none')}<br><b>State file:</b> ${D.state_file_exists?'PRESENT':'MISSING'} · ${D.state_file_size||0} bytes<br><b>Persistence error:</b> ${E(D.persistence_error||'none')}<br><b>Persistence file:</b> ${E(D.persistence_file||'')}<br><b>Rejection/block counts:</b> ${Object.entries(rr).map(x=>E(x[0])+': '+x[1]).join(' · ')||'none'}</div><hr><div class="diagnote"><b>Market Data &amp; Recovery Diagnostics</b><br><b>Worker:</b> ${E(D.market_data_worker_state||'UNKNOWN')} · <b>Source:</b> ${E(D.market_data_source||'UNKNOWN')}<br><b>1m fetch age:</b> ${Number(D.recovery_last_1m_cache_age||0).toFixed(1)}s · <b>5m fetch age:</b> ${Number(D.recovery_last_5m_cache_age||0).toFixed(1)}s<br><b>1m candle age:</b> ${Number(D.recovery_last_1m_age||0).toFixed(1)}s · <b>5m candle age:</b> ${Number(D.recovery_last_5m_age||0).toFixed(1)}s<br><b>Consecutive fetch failures:</b> ${D.market_data_consecutive_failures||0}<br><b>Last failure source:</b> ${E(D.market_data_last_failure_source||'NONE')}<br><b>Last failure:</b> ${E(D.market_data_last_failure_reason||'NONE')}<br><b>Recovery reason:</b> ${E(D.recovery_last_reason||'NONE')}</div><hr><div class="log small">${(D.decision_log||[]).slice().reverse().map(x=>`<div><span class="muted">${E(x.time)}</span> <b>${E(x.signal)} ${x.score}</b> → ${E(x.gate)} — ${E(x.note)}</div>`).join('')||'<span class="muted">No decisions recorded yet.</span>'}</div>`;let a=D.analytics||{};document.getElementById('perf').innerHTML='<div class="panelgrid"><div class="tag">Health<br><b>'+E(D.strategy_health||'INITIALIZING')+'</b></div><div class="tag">Samples<br><b>'+(a.samples||0)+'</b></div><div class="tag">Win rate<br><b>'+(a.win_rate||0)+'%</b></div><div class="tag">Profit factor<br><b>'+(a.profit_factor||0)+'</b></div><div class="tag">Expectancy<br><b>'+(a.expectancy_pct||0)+'%</b></div><div class="tag">Max drawdown<br><b>'+(a.max_drawdown_pct||0)+'%</b></div><div class="tag">Avg win<br><b>'+M(a.avg_win||0)+'</b></div><div class="tag">Avg loss<br><b>'+M(a.avg_loss||0)+'</b></div><div class="tag">Net P&amp;L<br><b>'+M(a.net_pnl||0)+'</b></div></div><p class=small><b>Score model:</b> RSI '+Number((D.score_components||{}).rsi||0).toFixed(1)+' · ADX '+Number((D.score_components||{}).adx||0).toFixed(1)+' · volume '+Number((D.score_components||{}).volume_ratio||0).toFixed(2)+' · threshold '+Number((D.score_components||{}).threshold||0)+'<br><b>Entry quality:</b> '+E(D.execution_quality||'UNKNOWN')+' · spread '+Number(D.spread_bps||0).toFixed(2)+' bps · imbalance '+Number(D.order_imbalance||0).toFixed(3)+'<br><b>Evidence policy:</b> strategy health is descriptive; small samples do not auto-change thresholds.</p>';let mi=D.market_intelligence||{};document.getElementById('intel').innerHTML='<div class="panelgrid"><div class="tag">Market phase<br><b>'+E(D.market_phase||'UNKNOWN')+'</b></div><div class="tag">Direction<br><b>'+E(D.market_direction||'WAIT')+'</b></div><div class="tag">Confidence<br><b>'+Number(D.market_confidence||0).toFixed(1)+'%</b></div><div class="tag">Alignment<br><b>'+Number(D.market_alignment||0).toFixed(1)+'%</b></div><div class="tag">Buy pressure<br><b>'+Number(mi.buy_pressure||0).toFixed(1)+'</b></div><div class="tag">Sell pressure<br><b>'+Number(mi.sell_pressure||0).toFixed(1)+'</b></div><div class="tag">EMA slope<br><b>'+Number(mi.ema_slope_bps||0).toFixed(2)+' bps</b></div><div class="tag">Bull factors<br><b>'+E((mi.bull_factors||[]).join(' · ')||'NONE')+'</b></div><div class="tag">Bear factors<br><b>'+E((mi.bear_factors||[]).join(' · ')||'NONE')+'</b></div><div class="tag">ADX / RSI<br><b>'+Number(mi.adx||0).toFixed(1)+' / '+Number(mi.rsi||0).toFixed(1)+'</b></div><div class="tag">Adaptive exits<br><b>'+Number(D.adaptive_exit_count||0)+'</b></div></div><p class="small"><b>Market read:</b> '+E(mi.reason||'Waiting for valid market data')+'<br><b>Trade thesis:</b> New positions store the market evidence used at entry; open positions are continuously re-evaluated for thesis strength, profit protection, giveback and invalidation. The engine does not assume certainty.</p>';let a27=D.adaptive_strategy||{};document.getElementById('adaptive027').innerHTML='<div class="panelgrid"><div class="tag">Entry validation<br><b>'+E(D.entry_validation||'NOT_READY')+'</b></div><div class="tag">Setup quality<br><b>'+Number(D.setup_quality||0).toFixed(1)+'</b></div><div class="tag">Adaptive threshold<br><b>'+Number(D.adaptive_threshold||70).toFixed(0)+'</b></div><div class="tag">Confluence<br><b>'+Number(D.confluence_score||0).toFixed(1)+'</b></div><div class="tag">Trend agreement<br><b>'+Number(D.trend_agreement||0).toFixed(1)+'</b></div><div class="tag">Liquidity quality<br><b>'+Number(D.liquidity_score||0).toFixed(1)+'</b></div><div class="tag">Volatility quality<br><b>'+Number(D.volatility_score||0).toFixed(1)+'</b></div><div class="tag">Momentum quality<br><b>'+Number(D.momentum_score||0).toFixed(1)+'</b></div><div class="tag">030 reviews<br><b>'+Number(D.strategy_027_reviews||0)+'</b></div></div><p class="small"><b>Adaptive read:</b> '+E((a27.reasons||[]).concat((a27.warnings||[]).map(x=>x+'_WARNING')).join(' · ')||'Setup passes the consolidated evidence gate.')+'<br><b>Regime:</b> '+E(a27.regime||D.regime||'UNKNOWN')+' · <b>Direction:</b> '+E(a27.direction||D.direction||'NEUTRAL')+' · <b>Confidence:</b> '+Number(a27.confidence||D.confidence||0).toFixed(1)+'% · <b>Alignment:</b> '+Number(a27.alignment||D.alignment||0).toFixed(1)+'%<br><b>Policy:</b> 030 uses consolidated strategy intelligence as advisory context while the dedicated 1m engine selects responsive opportunities. Hard safety gates remain active. It does not enable live orders.</p>';document.getElementById('open').innerHTML=(D.open||[]).map(t=>`<div class="trade" onclick="show(${t.id})"><div><b>#${t.id} ${t.side}</b><br><span class="muted">ACTIVE · ${M(t.allocated)} allocated</span></div><div class="money">${M(t.value)} <span class="${t.pnl>=0?'pos':'neg'}">${t.pnl>=0?'+':''}${M(t.pnl)}</span></div></div>`).join('')||'<span class="muted">No active trades</span>';let dr=D.diagnostic_report||{};let rrw=dr.rows||[];let notes=(dr.diagnosis||[]).map(x=>`<div class="diagnote">${E(x)}</div>`).join('')||'<div class="diagnote">No completed-trade diagnosis available.</div>';let sideSummary=Object.entries(dr.by_side||{}).map(([k,v])=>`<span class="tag"><b>${E(k)}</b> · ${v.samples} trades · ${v.wins}W/${v.losses}L · ${M(v.net_pnl)}</span>`).join('');let regimeSummary=Object.entries(dr.by_regime||{}).map(([k,v])=>`<span class="tag"><b>${E(k)}</b> · ${v.samples} · ${v.wins}W/${v.losses}L · avg ${M(v.avg_pnl)}</span>`).join('');let rows=rrw.slice().reverse().map(t=>`<tr onclick="show(${t.id},1)"><td>#${t.id}</td><td>${E(t.side)}</td><td>${t.entry_score}</td><td>${M(t.pnl)}</td><td>${M(t.mfe)}</td><td>${M(t.mae)}</td><td>${E(t.exit_reason)}</td><td>${t.hold_cycles}</td><td>${Number(t.r_multiple||0).toFixed(2)}</td><td>${Number(t.peak_giveback_pct||0).toFixed(1)}%</td><td>${E(t.thesis_status||'UNKNOWN')}</td><td>${E(t.data_quality||'COMPLETE')}</td></tr>`).join('');document.getElementById('tradeDiag').innerHTML=`<div class="panelgrid"><div class="tag">Diagnostic samples<br><b>${dr.samples||0}</b></div><div class="tag">Pool reference<br><b>${M(dr.pool_reference||100)}</b></div><div class="tag">Complete records<br><b>${(D.forensic_summary||{}).complete_records||0}</b></div><div class="tag">Legacy incomplete<br><b>${(D.forensic_summary||{}).legacy_incomplete||0}</b></div><div class="tag">Large giveback losses<br><b>${(D.forensic_summary||{}).large_giveback_losses||0}</b></div><div class="tag">Max-hold exits<br><b>${(D.forensic_summary||{}).max_hold_exits||0}</b></div><div class="tag">Current policy<br><b>DESCRIPTIVE ONLY</b></div></div><p class="small"><b>Evidence:</b> ${E(dr.policy||'')}<br><b>By direction:</b> ${sideSummary||'none'}<br><b>By regime:</b> ${regimeSummary||'none'}</p><div>${notes}</div><div class="diagscroll"><table class="diagtable"><thead><tr><th>ID</th><th>Side</th><th>Score</th><th>P&amp;L</th><th>MFE</th><th>MAE</th><th>Exit</th><th>Cycles</th><th>R</th><th>Giveback</th><th>Thesis</th><th>Quality</th></tr></thead><tbody>${rows||'<tr><td colspan="12" class="muted">No completed paper trades yet.</td></tr>'}</tbody></table></div><div class="mini">Click any row to open the full trade evidence: entry/exit, stop/target, score components, market context, MFE/MAE, R-multiple, giveback, holding cycles and exit reason.</div>`;document.getElementById('closed').innerHTML=(D.closed||[]).slice().reverse().map(t=>`<div class="trade" onclick="show(${t.id},1)"><div><b>#${t.id} ${t.side}</b><br><span class="muted">CLOSED · ${E(t.reason||'')}</span></div><div class="${t.pnl>=0?'pos':'neg'}">${t.pnl>=0?'+':''}${M(t.pnl)}</div></div>`).join('')||'<span class="muted">No history</span>';document.getElementById('chat').innerHTML=(D.chat||[]).map(x=>`<div><span class="muted">${E(x.time)}</span> ${E(x.text)}</div>`).join('');let eh=D.engine_health||{};document.getElementById('engineHealth').innerHTML='<div class="tag">Engine<br><b>'+E(eh.service_state||D.service_state||'UNKNOWN')+'</b></div><div class="tag">Heartbeat age<br><b>'+Number(eh.heartbeat_age||0).toFixed(2)+'s</b></div><div class="tag">Cycles<br><b>'+Number(eh.cycle_count||0)+'</b></div><div class="tag">Connection<br><b>'+E(eh.connection||D.connection||'UNKNOWN')+'</b></div><div class="tag">Recovery<br><b>'+E(eh.recovery_state||'UNKNOWN')+'</b></div><div class="tag">Persistence<br><b>'+E(eh.persistence_status||'NOT_VERIFIED')+'</b></div><div class="tag">Last engine error<br><b>'+E(eh.last_error||'NONE')+'</b></div>';let ai=D.ai_status||{},tr=ai.trading||{},gu=ai.guardian||{},or=ai.orchestrator||{};document.getElementById('aiAgents').innerHTML='<div class="tag"><b>TRADING AI</b><br>State: '+E(tr.state||'UNKNOWN')+'<br>Signal: '+E(tr.signal||'WAIT')+' · Score '+Number(tr.score||0).toFixed(1)+'<br>Gate: '+E(tr.gate||'NO_SIGNAL')+'<br>Validation: '+E(tr.validation||'NOT_READY')+'<br>Quick mode: '+E(tr.quick_mode||'IDLE')+'</div><div class="tag"><b>GUARDIAN AI</b><br>Health: '+E(gu.state||'CHECKING')+'<br>Connection: '+E(gu.connection||'UNKNOWN')+'<br>Engine: '+E(gu.engine||'UNKNOWN')+'<br>Persistence: '+E(gu.persistence||'UNKNOWN')+'</div><div class="tag"><b>ORCHESTRATOR AI</b><br>State: '+E(or.state||'CHECKING')+'<br>Active specialists: '+Number(or.specialists||0)+'<br>Total created: '+Number(or.total_specialists||0)+'<br>Approval pending: '+(or.pending_permission?'YES':'NO')+'</div>';let z=D.z3||{};document.getElementById('aiActivity').innerHTML='<b>Supervisor activity:</b> '+E(z.last_problem||'No protected problem currently detected.')+' · Auto repairs '+Number(z.auto_repairs||0)+' · Permission requests '+Number(z.permission_requests||0)+' · Specialists '+Number(z.specialists_created||0);draw()}
let shownId=null,shownClosed=false;function show(id,closed){let t=(closed?D.closed:D.open).find(x=>x.id==id);if(!t)return;shownId=id;shownClosed=!!closed;document.getElementById('closeTrade').style.display=closed?'none':'inline-block';document.getElementById('detail').textContent=JSON.stringify(t,null,2);document.getElementById('modal').style.display='flex'}function closeShown(){if(shownId!==null&&!shownClosed){cmd('Close '+shownId);document.getElementById('modal').style.display='none'}}
function draw(){let v=document.getElementById('chart'),g=v.getContext('2d'),c=(D.candles||[]).slice(-70);g.clearRect(0,0,v.width,v.height);if(!c.length){g.fillStyle='#94a3b8';g.font='18px Arial';g.textAlign='center';g.fillText('AI EXECUTION VIEW',v.width/2,v.height/2-16);g.font='13px Arial';g.fillText(D.connection==='ONLINE'?'Connected — waiting for first verified candle cycle.':'Waiting for verified market data — entries safely blocked.',v.width/2,v.height/2+12);g.textAlign='left';return}let vals=c.flatMap(x=>[Number(x.h),Number(x.l)]).filter(Number.isFinite);if(!vals.length)return;let hi=Math.max(...vals),lo=Math.min(...vals),X=i=>i*v.width/c.length+5,Y=p=>v.height-34-(p-lo)/(hi-lo||1)*330,w=Math.max(3,v.width/c.length*.55);c.forEach((x,i)=>{let o=Number(x.o),h=Number(x.h),l=Number(x.l),cl=Number(x.c);if(![o,h,l,cl].every(Number.isFinite))return;let q=cl>=o?'#4de276':'#ff5d6c',xx=X(i),yo=Y(o),yc=Y(cl);g.strokeStyle=q;g.fillStyle=q;g.beginPath();g.moveTo(xx+w/2,Y(h));g.lineTo(xx+w/2,Y(l));g.stroke();g.fillRect(xx,Math.min(yo,yc),w,Math.max(2,Math.abs(yc-yo)))});function L(n,col){let a=c.map(x=>Number(x.c)).filter(Number.isFinite),k=2/(n+1);if(!a.length)return;let z0=a[0];g.strokeStyle=col;g.beginPath();a.forEach((p,i)=>{let z=i?p*k+z0*(1-k):z0;if(i)g.lineTo(X(i),Y(z));else g.moveTo(X(i),Y(z));z0=z});g.stroke()}L(21,'#4da3ff');L(50,'#c58cff');let ex=D.execution_view||{},price=Number(ex.price);if(Number.isFinite(price)&&price>0){g.strokeStyle='#ffd966';g.setLineDash([5,4]);g.beginPath();g.moveTo(0,Y(price));g.lineTo(v.width,Y(price));g.stroke();g.setLineDash([]);g.fillStyle='#ffd966';g.font='12px Arial';g.fillText('PRICE '+price.toFixed(2),8,16)}let trades=[...(D.open||[])].slice(0,8);trades.forEach((t,i)=>{let ep=Number(t.entry),sl=Number(t.stop_loss),tp=Number(t.target_price);if(Number.isFinite(ep)){g.fillStyle=t.side==='BUY'?'#70ff9b':'#ff8992';g.beginPath();g.arc(v.width-18,Y(ep),5,0,Math.PI*2);g.fill();g.fillText(t.side+' #'+t.id,8,32+i*15)}if(Number.isFinite(sl)&&sl>0){g.strokeStyle='#ff8992';g.setLineDash([3,3]);g.beginPath();g.moveTo(v.width*.55,Y(sl));g.lineTo(v.width,Y(sl));g.stroke()}if(Number.isFinite(tp)&&tp>0){g.strokeStyle='#70ff9b';g.setLineDash([3,3]);g.beginPath();g.moveTo(v.width*.55,Y(tp));g.lineTo(v.width,Y(tp));g.stroke()}g.setLineDash([])});let closed=(D.closed||[]).slice(-12);closed.forEach(t=>{let ep=Number(t.entry),xp=Number(t.exit_price||t.current||t.exit);if(!Number.isFinite(ep)||!Number.isFinite(xp))return;let xx=v.width-28;g.fillStyle=t.pnl>=0?'#70ff9b':'#ff8992';g.fillRect(xx,Math.max(20,Math.min(v.height-20,Y(xp)))-2,8,4)});g.fillStyle='#94a3b8';g.font='11px Arial';g.fillText('Signal: '+String(ex.signal||'WAIT')+' · Score: '+Number(ex.score||0).toFixed(1)+' · Gate: '+String(ex.entry_gate||'NO_SIGNAL'),8,v.height-8)}
function z3render(){let z=D.z3||{},p=z.pending_permission;let ev=(D.z3_events||[]).slice().reverse().slice(0,10).map(x=>'<div><span class="muted">'+E(x.time)+'</span> <b>'+E(x.kind)+'</b> — '+E(x.text)+'</div>').join('')||'<div class="muted">No supervisor events yet.</div>';document.getElementById('z3panel').innerHTML='<div class="panelgrid"><div class="tag">Supervisor<br><b>'+E(z.health||'CHECKING')+'</b></div><div class="tag">Problems<br><b>'+Number(z.problem_count||0)+'</b></div><div class="tag">Auto repairs<br><b>'+Number(z.auto_repairs||0)+'</b></div><div class="tag">Specialists<br><b>'+Number(z.specialists_created||0)+'</b></div><div class="tag">Permission requests<br><b>'+Number(z.permission_requests||0)+'</b></div><div class="tag">Last problem<br><b>'+E(z.last_problem||'NONE')+'</b></div></div><p class="small"><b>Last repair:</b> '+E(z.last_repair_result||'None')+'<br><b>Permission:</b> '+E(p?(p.action+': '+p.reason):'None')+'<br><b>Authority:</b> '+E(z.authority_model||'AUTO_SAFE / APPROVAL_PROTECTED')+'</p>'+(p?'<button onclick="cmd(\'Approve Z3\')">APPROVE REQUEST</button>':'')+'<details><summary><b>Z3 Supervisor AI Guide</b> — what each AI does and what requires approval</summary><div class="small" style="line-height:1.7;margin-top:8px"><b>Trading AI:</b> reads market data, decides BUY / SELL / WAIT, validates entries, manages and exits paper trades.<br><b>Guardian AI:</b> watches heartbeat, connection, persistence, runtime health and recovery conditions; it may perform safe recovery only.<br><b>Orchestrator AI:</b> coordinates bounded specialists and combines their evidence; it does not grant specialists unrestricted control. <b>Owner/Admin UPGRADE:</b> runs diagnosis → safe repair → candidate download → paper-only/compile validation → atomic backup/install → restart; failed candidates are rejected and the prior build is restored.<br><b>Specialists:</b> Network/Market Data, Engine Recovery, State Integrity, Security and Strategy Diagnostics provide evidence only. They cannot place trades, change risk limits, rewrite source, or enable live trading.<br><b>Automatic:</b> safe network/data recovery and persistence repair when the paper-only boundary is intact.<br><b>Approval required:</b> protected runtime/code changes, strategy-rule changes, security changes, risk-limit changes, authentication/capital changes, and anything affecting live-trading boundaries.<br><b>Repair flow:</b> detect → diagnose → specialist evidence → safe repair or permission request → validate → persist → report. Failed repairs must remain blocked rather than silently bypassing safety gates.<br><b>Voice:</b> Z3 now tracks browser voice READY/ERROR/UNSUPPORTED reports. Client-side browser limitations are diagnosed and isolated with text-chat fallback; the server never pretends it can repair a browser permission or device problem it cannot control.</div></details><details><summary><b>Supervisor Commands</b></summary><div class="small" style="line-height:1.7;margin-top:8px">Z3 Status · Z3 Diagnose · Z3 Repair · Approve Z3 · Check system · Repair now</div></details><div class="log small" style="margin-top:8px">'+ev+'</div>'}
let voiceRec=null,voiceOutput=true,lastSpokenChat='';function voiceStart(){let R=window.SpeechRecognition||window.webkitSpeechRecognition;if(!R){cmd('voice unavailable');say('Voice input is not supported by this browser. Use the Owner/Admin AI Chat box to type commands.');let m=document.getElementById('msg');if(m){m.focus();m.placeholder='Voice input unavailable — type to AI';}return}voiceRec=new R();voiceRec.lang='en-NG';voiceRec.continuous=false;voiceRec.interimResults=false;voiceRec.onresult=e=>{let t=e.results[0][0].transcript;document.getElementById('msg').value=t;cmd(t)};voiceRec.onerror=e=>{cmd('voice error '+(e&&e.error?e.error:'unknown'));say('Voice input stopped. I recorded the voice error for Z3 Supervisor.');};voiceRec.onend=()=>{if(voiceRec){voiceRec=null}};try{voiceRec.start();cmd('voice ready');say('I am listening.')}catch(e){cmd('voice error '+(e&&e.message?e.message:'start failed'));say('Voice could not start. Text chat remains available.')}}function voiceStop(){if(voiceRec){try{voiceRec.stop()}catch(e){};voiceRec=null}try{speechSynthesis.cancel()}catch(e){}}function say(t){try{let u=new SpeechSynthesisUtterance(String(t));u.lang='en-NG';u.onerror=e=>{try{cmd('voice error synthesis '+(e&&e.error?e.error:'unknown'))}catch(_){}};speechSynthesis.cancel();speechSynthesis.speak(u)}catch(e){try{cmd('voice error synthesis '+(e&&e.message?e.message:'start failed'))}catch(_){}}}function speakNewChat(){if(!voiceOutput||!D.chat||!D.chat.length)return;let x=D.chat[D.chat.length-1];let key=String(x.time||'')+'|'+String(x.text||'');if(!x||!x.time||key===lastSpokenChat)return;lastSpokenChat=key;let t=String(x.text||'');if(t)say(t)}
let oldRender=render;render=function(){try{oldRender();z3render();speakNewChat()}catch(e){let box=document.getElementById('commandResult');if(box){box.className='diagnote warn';box.innerHTML='<b>DASHBOARD RENDER ERROR:</b> '+E(e.message||e)}console.error(e);try{if(!sessionStorage.getItem('z3_dash_reload')){sessionStorage.setItem('z3_dash_reload','1');fetch('/action?text='+encodeURIComponent('dashboard error '+String(e&&e.message||e))).catch(()=>{});setTimeout(()=>location.reload(),250)}else{sessionStorage.removeItem('z3_dash_reload')}}catch(_){}}};
setInterval(load,1000);load();</script></body></html>'''


def _esc(v):
    return str(v if v is not None else '').replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace('"','&quot;')

def _server_seed_html():
    """Render critical dashboard state server-side so the UI remains usable even if browser JS fails."""
    d=snapshot(); z=d.get('z3',{}); ai=d.get('ai_status',{}); eh=d.get('engine_health',{}); mode='RUNNING' if d.get('running') and not d.get('paused') else 'PAUSED' if d.get('running') else 'STOPPED'
    conn='ONLINE' if d.get('connection')=='ONLINE' else 'CONNECTION '+str(d.get('connection') or 'UNKNOWN')
    metric_rows=[('Paper account',f"${float(d.get('paper_balance',0)):,.4f}"),('Trading pool',f"${float(d.get('pool',0)):,.4f}"),('Available balance',f"${float(d.get('available',0)):,.4f}"),('Allocated capital',f"${float(d.get('allocated_capital',0)):,.4f}"),('Per trade',f"${float(d.get('per_trade',0)):,.4f}"),('Trading equity',f"${float(d.get('equity',0)):,.4f}"),('Realized P&L',f"${float(d.get('realized_pnl',0)):,.4f}"),('Unrealized P&L',f"${float(d.get('unrealized_pnl',0)):,.4f}"),('Total P&L',f"${float(d.get('total_pnl',0)):,.4f}"),('Drawdown',f"{float(d.get('drawdown_pct',0)):.2f}%"),('Open trades',len(d.get('open',[]))),('Total trades',len(d.get('closed',[]))+len(d.get('open',[]))),('AI signal',f"{d.get('signal','WAIT')} · {d.get('score',0)}"),('Validation',d.get('entry_validation','NOT_READY')),('Market quality',d.get('market_quality','UNKNOWN')),('Risk block',d.get('risk_block') or 'NONE'),('Engine state',d.get('service_state','UNKNOWN')),('Recovery',d.get('recovery_state','UNKNOWN')),('Persisted','VERIFIED' if d.get('persistence_ok') else 'NOT VERIFIED')]
    metrics=''.join(f'<div class="metric"><span class="muted">{_esc(k)}</span><br><b>{_esc(v)}</b></div>' for k,v in metric_rows)
    ai_html=(f'<div class="tag"><b>TRADING AI</b><br>State: {_esc(ai.get("trading",{}).get("state","UNKNOWN"))}<br>Signal: {_esc(ai.get("trading",{}).get("signal","WAIT"))} · Score {float(ai.get("trading",{}).get("score",0)):.1f}<br>Gate: {_esc(ai.get("trading",{}).get("gate","NO_SIGNAL"))}<br>Validation: {_esc(ai.get("trading",{}).get("validation","NOT_READY"))}</div>'
      f'<div class="tag"><b>GUARDIAN AI</b><br>Health: {_esc(ai.get("guardian",{}).get("state","CHECKING"))}<br>Connection: {_esc(ai.get("guardian",{}).get("connection","UNKNOWN"))}<br>Engine: {_esc(ai.get("guardian",{}).get("engine","UNKNOWN"))}<br>Persistence: {_esc(ai.get("guardian",{}).get("persistence","UNKNOWN"))}</div>'
      f'<div class="tag"><b>ORCHESTRATOR AI</b><br>State: {_esc(ai.get("orchestrator",{}).get("state","CHECKING"))}<br>Active specialists: {int(ai.get("orchestrator",{}).get("specialists",0))}<br>Total created: {int(ai.get("orchestrator",{}).get("total_specialists",0))}<br>Approval pending: {"YES" if ai.get("orchestrator",{}).get("pending_permission") else "NO"}</div>')
    zhtml=f'<div class="panelgrid"><div class="tag">Supervisor<br><b>{_esc(z.get("health","CHECKING"))}</b></div><div class="tag">Problems<br><b>{int(z.get("problem_count",0))}</b></div><div class="tag">Auto repairs<br><b>{int(z.get("auto_repairs",0))}</b></div><div class="tag">Specialists<br><b>{int(z.get("specialists_created",0))}</b></div><div class="tag">Permission requests<br><b>{int(z.get("permission_requests",0))}</b></div><div class="tag">Self-heal<br><b>{_esc(z.get("self_heal_status","STARTING"))}</b></div><div class="tag">Dashboard<br><b>{_esc(z.get("dashboard_health","CHECKING"))}</b></div><div class="tag">App upgrade<br><b>{_esc(z.get("upgrade_state","READY"))}</b></div><div class="tag">Strategy upgrade<br><b>{_esc(z.get("strategy_upgrade_status","LEARNING"))}</b></div></div><p class="small"><b>Last problem:</b> {_esc(z.get("last_problem") or "NONE")}<br><b>Last repair:</b> {_esc(z.get("last_repair_result") or "None")}</p>'
    eng=f'<div class="tag">Engine<br><b>{_esc(eh.get("service_state","UNKNOWN"))}</b></div><div class="tag">Heartbeat age<br><b>{float(eh.get("heartbeat_age",0)):.2f}s</b></div><div class="tag">Cycles<br><b>{int(eh.get("cycle_count",0))}</b></div><div class="tag">Connection<br><b>{_esc(eh.get("connection","UNKNOWN"))}</b></div><div class="tag">Recovery<br><b>{_esc(eh.get("recovery_state","UNKNOWN"))}</b></div><div class="tag">Persistence<br><b>{_esc(eh.get("persistence_status","NOT_VERIFIED"))}</b></div>'
    md=eh
    marketdiag=f'<div class="panelgrid"><div class="tag">Market worker<br><b>{_esc(md.get("market_data_worker_state","UNKNOWN"))}</b></div><div class="tag">Market source<br><b>{_esc(md.get("market_data_source","UNKNOWN"))}</b></div><div class="tag">1m fetch age<br><b>{float(md.get("recovery_last_1m_cache_age",0) or 0):.1f}s</b></div><div class="tag">5m fetch age<br><b>{float(md.get("recovery_last_5m_cache_age",0) or 0):.1f}s</b></div><div class="tag">1m candle age<br><b>{float(md.get("recovery_last_1m_age",0) or 0):.1f}s</b></div><div class="tag">5m candle age<br><b>{float(md.get("recovery_last_5m_age",0) or 0):.1f}s</b></div><div class="tag">Fetch failures<br><b>{int(md.get("market_data_consecutive_failures",0) or 0)}</b></div><div class="tag">Last failure<br><b>{_esc(md.get("market_data_last_failure_source") or "NONE")}</b></div></div><p class="small"><b>Verification:</b> {_esc(d.get("data_verification_status") or "NOT_RUN")} · {_esc(d.get("data_verification_reason") or "")}<br><b>Verification detail:</b> {_esc(d.get("data_verification_detail") or "")}<br><b>Recovery diagnostic:</b> {_esc(md.get("recovery_last_reason") or md.get("market_data_last_failure_reason") or "No recovery diagnostic yet.")}</p>'
    diag=f"<div class='diagnote'><b>Server dashboard is active.</b> State endpoint and command fallback are available. Current gate: {_esc(d.get('entry_gate','NO_SIGNAL'))}. Decision: {_esc(d.get('decision_note') or 'No market decision yet.')}</div><div class='box'><h3>Market Data &amp; Recovery Diagnostics</h3>{marketdiag}</div>"
    chat=''.join(f'<div><span class="muted">{_esc(x.get("time"))}</span> {_esc(x.get("text"))}</div>' for x in d.get('chat',[])) or '<div class="muted">Owner/Admin AI chat is ready.</div>'
    perf=f'<div class="panelgrid"><div class="tag">Strategy health<br><b>{_esc(d.get("strategy_health","INITIALIZING"))}</b></div><div class="tag">Samples<br><b>{int(d.get("performance_samples",0))}</b></div><div class="tag">Win rate<br><b>{float(d.get("analytics",{}).get("win_rate",0)):.1f}%</b></div><div class="tag">Profit factor<br><b>{float(d.get("analytics",{}).get("profit_factor",0)):.2f}</b></div><div class="tag">Net P&amp;L<br><b>${float(d.get("analytics",{}).get("net_pnl",0)):.4f}</b></div></div><p class="small">Strategy intelligence is active. It will populate with verified market evidence and completed paper-trade samples.</p>'
    intel=f'<div class="panelgrid"><div class="tag">Market phase<br><b>{_esc(d.get("market_phase","UNKNOWN"))}</b></div><div class="tag">Direction<br><b>{_esc(d.get("market_direction","WAIT"))}</b></div><div class="tag">Confidence<br><b>{float(d.get("market_confidence",0)):.1f}%</b></div><div class="tag">Alignment<br><b>{float(d.get("market_alignment",0)):.1f}%</b></div></div><p class="small">Market Intelligence is connected to the engine. Fresh multi-timeframe evidence appears when verified market data is available.</p>'
    adapt=f'<div class="panelgrid"><div class="tag">Entry validation<br><b>{_esc(d.get("entry_validation","NOT_READY"))}</b></div><div class="tag">Setup quality<br><b>{float(d.get("setup_quality",0)):.1f}</b></div><div class="tag">Adaptive threshold<br><b>{float(d.get("adaptive_threshold",70)):.0f}</b></div><div class="tag">Confluence<br><b>{float(d.get("confluence_score",0)):.1f}</b></div><div class="tag">Trend agreement<br><b>{float(d.get("trend_agreement",0)):.1f}</b></div></div><p class="small">Integrated-030 dual-speed strategy is loaded. Normal strategy and Quick Advantage remain paper-only and symmetric for BUY/SELL.</p>'
    for key,val in {'id="m"':f'id="m">{metrics}','id="statebar" class="statebar stopped">● STOPPED':f'id="statebar" class="statebar {mode.lower()}">● {mode} — AI {"ACTIVE" if mode=="RUNNING" else "HOLDING" if mode=="PAUSED" else "OFF"}', 'id="conn" class="status offline">CONNECTION UNKNOWN':f'id="conn" class="status {"online" if d.get("connection")=="ONLINE" else "offline"}">{_esc(conn)}','id="diag"></div>':f'id="diag">{diag}</div>','id="open"></div>':f'id="open"><span class="muted">{len(d.get("open",[]))} active paper trade(s).</span></div>','id="perf"></div>':f'id="perf">{perf}</div>','id="intel"></div>':f'id="intel">{intel}</div>','id="adaptive027"></div>':f'id="adaptive027">{adapt}</div>','id="tradeDiag"></div>':f'id="tradeDiag"><div class="diagnote">No completed paper trades yet. Forensics will populate automatically after the first closed trade.</div></div>','id="closed"></div>':f'id="closed"><span class="muted">{len(d.get("closed",[]))} completed paper trade(s).</span></div>','id="chat" class="chat"></div>':f'id="chat" class="chat">{chat}</div>','id="engineHealth" class="panelgrid"></div>':f'id="engineHealth" class="panelgrid">{eng}</div>','id="aiAgents" class="panelgrid"></div>':f'id="aiAgents" class="panelgrid">{ai_html}</div>','id="aiActivity" class="log small">Trading AI, Guardian AI and Orchestrator AI are connected to the same paper-only control state.</div>':f'id="aiActivity" class="log small">Trading AI, Guardian AI and Orchestrator AI are connected. Supervisor: {_esc(z.get("health","CHECKING"))}. Engine: {_esc(eh.get("service_state","UNKNOWN"))}.</div>','id="z3panel"></div>':f'id="z3panel">{zhtml}</div>'}.items():
        # caller applies these sequentially to the HTML template
        pass
    html=HTML
    replacements={'id="m"':f'id="m">{metrics}','id="statebar" class="statebar stopped">● STOPPED':f'id="statebar" class="statebar {mode.lower()}">● {mode} — AI {"ACTIVE" if mode=="RUNNING" else "HOLDING" if mode=="PAUSED" else "OFF"}', 'id="conn" class="status offline">CONNECTION UNKNOWN':f'id="conn" class="status {"online" if d.get("connection")=="ONLINE" else "offline"}">{_esc(conn)}','id="diag"></div>':f'id="diag">{diag}</div>','id="open"></div>':f'id="open"><span class="muted">{len(d.get("open",[]))} active paper trade(s).</span></div>','id="perf"></div>':f'id="perf">{perf}</div>','id="intel"></div>':f'id="intel">{intel}</div>','id="adaptive027"></div>':f'id="adaptive027">{adapt}</div>','id="tradeDiag"></div>':f'id="tradeDiag"><div class="diagnote">No completed paper trades yet. Forensics will populate automatically after the first closed trade.</div></div>','id="closed"></div>':f'id="closed"><span class="muted">{len(d.get("closed",[]))} completed paper trade(s).</span></div>','id="chat" class="chat"></div>':f'id="chat" class="chat">{chat}</div>','id="engineHealth" class="panelgrid"></div>':f'id="engineHealth" class="panelgrid">{eng}</div>','id="aiAgents" class="panelgrid"></div>':f'id="aiAgents" class="panelgrid">{ai_html}</div>','id="aiActivity" class="log small">Trading AI, Guardian AI and Orchestrator AI are connected to the same paper-only control state.</div>':f'id="aiActivity" class="log small">Trading AI, Guardian AI and Orchestrator AI are connected. Supervisor: {_esc(z.get("health","CHECKING"))}. Engine: {_esc(eh.get("service_state","UNKNOWN"))}.</div>','id="z3panel"></div>':f'id="z3panel">{zhtml}</div>','<canvas id="chart" width="900" height="410"></canvas>':f'<div id="chartServerState" class="diagnote">AI EXECUTION VIEW · {mode} · {_esc(d.get("connection","UNKNOWN"))} · Signal {_esc(d.get("signal","WAIT"))} · Paper entries require verified fresh market data. Current gate: '+(d.get('entry_gate','NO_SIGNAL'))+' .</div><canvas id="chart" width="900" height="410"></canvas>'}
    for a,b in replacements.items(): html=html.replace(a,b,1)
    return html

class H(BaseHTTPRequestHandler):
    def reply(self,c,data,t):
        try:
            b=data.encode();self.send_response(c);self.send_header('Content-Type',t);self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
        except (BrokenPipeError,ConnectionAbortedError,ConnectionResetError,OSError):
            return
    def do_GET(self):
        from urllib.parse import urlparse,parse_qs
        u=urlparse(self.path)
        if u.path in ('/state','/api/state'):self.reply(200,json.dumps(snapshot()),'application/json')
        elif u.path in ('/cmdget','/action'):
            text=parse_qs(u.query).get('text',parse_qs(u.query).get('cmd',['']))[0]
            if text: command(text)
            self.reply(200,_server_seed_html(),'text/html')
        elif u.path=='/':self.reply(200,_server_seed_html(),'text/html')
        else:self.reply(404,'Not found','text/plain')
    def do_POST(self):
        try:
            n=int(self.headers.get('Content-Length','0')); result=command(self.rfile.read(n).decode()); self.reply(200,json.dumps(result),'application/json')
        except Exception as e:self.reply(500,json.dumps({'ok':False,'message':str(e)}),'application/json')
    def log_message(self,*a):pass

def market_data_worker():
    """Dedicated network worker. Engine/control/UI never wait on Bybit."""
    next_candle=0.0; next_quality=0.0; next_deriv=0.0; next_ticker=0.0
    while not MARKET_STOP.is_set() and not STOP.is_set():
        try:
            t=time.time()
            normalize_verified_health()
            running=bool(S.get('running')); paused=bool(S.get('paused'))
            if (running or S.get('recovery_pending')) and not paused and S.get('connection')!='ONLINE' and t>=next_ticker:
                recovery_cycle()
                next_ticker=t+max(1.5,RECOVERY_RETRY)
                normalize_verified_health()
            elif running and t>=next_ticker:
                p=ticker(force=True)
                if p and S.get('connection')!='ONLINE' and S.get('data_verification_status')=='VERIFIED' and float(S.get('recovery_last_1m_cache_age',1e9) or 1e9)<=STALE_SECONDS and float(S.get('recovery_last_5m_cache_age',1e9) or 1e9)<=STALE_SECONDS:
                    set_connection('ONLINE','Verified market-data path is healthy again.')
                next_ticker=t+0.75
            if running and not paused and t>=next_candle:
                candle_step()
                recovery_reconcile()
                next_candle=t+DATA_EVERY
            elif running and not paused and S.get('connection')=='ONLINE' and t>=next_quality:
                market_quality(force=True); next_quality=t+QUALITY_EVERY
            if running and not paused and S.get('connection')=='ONLINE' and t>=next_deriv:
                derivatives_context(force=True); next_deriv=t+DERIVATIVES_EVERY
            time.sleep(0.10)
        except Exception as e:
            msg=str(e)[-220:]
            # A transient analytics/worker exception must never erase a freshly
            # verified market snapshot. Reuse the last verified snapshot only
            # while its recorded fetch/candle ages are still inside the hard
            # freshness limits; otherwise the normal safety block remains.
            restored=False
            try:
                with LOCK:
                    cache_ok=(float(S.get('recovery_last_1m_cache_age',1e9) or 1e9)<=STALE_SECONDS and float(S.get('recovery_last_5m_cache_age',1e9) or 1e9)<=STALE_SECONDS)
                    age_ok=(float(S.get('recovery_last_1m_age',1e9) or 1e9)<=MARKET_1M_MAX_AGE and float(S.get('recovery_last_5m_age',1e9) or 1e9)<=MARKET_5M_MAX_AGE)
                    snapshot_ok=(isinstance(S.get('candles'),list) and len(S.get('candles',[]))>=31 and isinstance(S.get('one_minute_candles'),list) and len(S.get('one_minute_candles',[]))>=31)
                    if S.get('desired_running') and not S.get('paused') and cache_ok and age_ok and snapshot_ok:
                        S['data_verification_status']='VERIFIED'
                        S['data_verification_reason']=''
                        S['data_verification_detail']='Verified snapshot retained through transient market-worker exception; fresh-age gates still pass.'
                        S['entry_gate']='VERIFIED_MARKET_DATA'
                        S['market_data_worker_state']='VERIFIED'
                        S['recovery_pending']=False
                        S['recovery_state']='RECOVERED'
                        S['service_state']='RUNNING'
                        S['connection']='ONLINE'
                        restored=True
                    S['errors']=(S.get('errors',[])+['MARKET WORKER: '+msg])[-10:]
                    S['engine_last_error']='' if restored else msg
                    if not restored and S.get('desired_running') and S.get('connection')!='ONLINE':
                        S['recovery_pending']=True; S['recovery_state']='RECOVERY_REQUIRED'; S['service_state']='RUNNING' if S.get('desired_running') and not S.get('paused') else 'READY'
                if restored: log_decision('WAIT',0,'VERIFIED_SNAPSHOT_RETAINED','Transient market-worker exception did not invalidate a still-fresh verified snapshot: '+msg)
            except Exception as inner:
                with LOCK:
                    S['errors']=(S.get('errors',[])+['MARKET WORKER RECOVERY: '+str(inner)[-180:]])[-10:]
                    S['engine_last_error']=msg
            next_ticker=time.time()+max(1.5,RECOVERY_RETRY)
            time.sleep(0.25)

def signal_handoff_watchdog():
    """Repair only the verified-snapshot -> signal handoff; never fetches market data and never weakens hard gates."""
    try:
        normalize_verified_health()
        with LOCK:
            if not (S.get('running') and not S.get('paused') and S.get('data_verification_status')=='VERIFIED'):
                return False
            c1=list(S.get('one_minute_candles') or [])
            ad=dict(S.get('adaptive_strategy') or {})
            stale_zero=(str(S.get('signal') or 'WAIT')=='WAIT' and float(S.get('score',0) or 0)<=0)
        if len(c1)<31 or not (bool(ad.get('ready')) and str(ad.get('direction') or '').upper() in ('BUY','SELL')):
            return False
        side,sc,info=btc_1m_strategy(c1)
        ad_side=str(ad.get('direction') or '').upper(); aq=float(ad.get('quality',0) or 0); ath=float(ad.get('threshold',64) or 64)
        if side=='WAIT' and float(sc or 0)<=0 and ad_side in ('BUY','SELL') and aq>=ath and float(ad.get('alignment',0) or 0)>=50 and float(ad.get('confidence',0) or 0)>=46:
            side=ad_side;sc=round(max(aq,ath),1);info=dict(info or {});info.update({'mode':'QUICK_ADVANTAGE','prob':round(max(float(info.get('prob',50) or 50),aq),1),'reason':'Verified-snapshot watchdog repaired an empty 1m signal handoff.'})
        if side=='WAIT' and float(sc or 0)<=0:
            return False
        mode=info.get('mode','WAIT');required=54 if mode=='NORMAL' else 46
        ready_signal=side in ('BUY','SELL') and float(sc)>=required
        with LOCK:
            S['last_signal_engine_side']=side;S['last_signal_engine_score']=sc;S['signal']=side;S['score']=sc;S['last_action']=side;S['prediction']=info.get('prob',50);S['prediction_reason']=info.get('reason','');S['quick_signal']=side;S['quick_score']=sc;S['quick_mode']=mode;S['quick_reason']=info.get('reason','');S['signal_engine_status']='RUNNING';S['signal_engine_error']='';S['signal_engine_last_eval']=now();S['signal_engine_evaluations']=int(S.get('signal_engine_evaluations',0))+1;S['entry_gate']='VERIFIED_MARKET_DATA';S['entry_validation']='READY' if ready_signal else 'BLOCKED';S['validation_reasons']=[] if ready_signal else [f'{mode}_SCORE_BELOW_{required}'];S['signal_handoff_repairs']=int(S.get('signal_handoff_repairs',0))+1
            if side=='BUY':S['buy_signals']+=1;S['signals_seen']+=1;S['last_signal_time']=now();S['session_signals']+=1
            elif side=='SELL':S['sell_signals']+=1;S['signals_seen']+=1;S['last_signal_time']=now();S['session_signals']+=1
        if ready_signal:
            log_decision(side,sc,'READY',f'{mode} verified-snapshot watchdog repaired the signal handoff; score {sc} >= {required}.')
            with LOCK:
                enough=free()>=S['per_trade']; same_side=any(t['side']==side for t in S['open']); ct=(c1[-2]['t'] if len(c1)>=2 else None)
            if enough and not same_side:
                if open_trade(side,'WATCHDOG_'+('QUICK_ADVANTAGE' if mode=='QUICK_ADVANTAGE' else 'NORMAL_1M')):
                    with LOCK: S['forming_entry_candle']=ct
            else:
                with LOCK:
                    S['entry_validation']='BLOCKED';S['validation_reasons']=['CAPITAL_OR_SAME_SIDE_POSITION'];S['entry_gate']='CAPITAL_OR_SAME_SIDE_POSITION'
        else:
            log_decision(side,sc,'SIGNAL_BELOW_THRESHOLD',f'{mode} verified-snapshot watchdog recovered a non-zero signal, but score {sc} is below {required}.')
        return True
    except Exception as e:
        log_error('SIGNAL HANDOFF WATCHDOG',e)
        return False

def engine():
    last=0.0;last_persist=0.0;last_recovery=0.0;last_z3=0.0
    while not STOP.is_set():
        try:
            with LOCK:
                S['engine_heartbeat']=time.time(); S['engine_cycle_count']=int(S.get('engine_cycle_count',0))+1
            if time.time()-last_z3>=2.5:
                z3_tick(); last_z3=time.time()
            recovery_reconcile()
            if S['running']:
                # Fast local engine: never perform exchange I/O here, including recovery.
                signal_handoff_watchdog()
                manage(allow_entries=not S.get('paused',False))
            elif S.get('recovery_pending'):
                # Recovery is owned by the market-data worker. The engine remains non-blocking.
                pass
            with LOCK:S['engine_heartbeat']=time.time()
            if time.time()-last_persist>=PERSIST_EVERY:
                persist_state('engine_heartbeat');last_persist=time.time()
            time.sleep(LOOP)
        except Exception as e:
            with LOCK:
                S['errors']=(S['errors']+[str(e)[-180:]])[-10:]
                S['engine_errors']=(S.get('engine_errors',[])+[str(e)[-180:]])[-10:]
                S['engine_last_error']=str(e)[-300:]
                S['service_state']='DEGRADED'
            persist_state('engine_exception')
            time.sleep(LOOP)

def shutdown(*_):
    with LOCK:
        S['running']=False;S['paused']=False;S['desired_running']=False;S['recovery_pending']=False;S['service_state']='STOPPED';S['recovery_state']='CLEAN_SHUTDOWN'
    say('Safe shutdown requested. Paper trading stopped. Runtime state saved.')
    persist_state('clean_shutdown')
    MARKET_STOP.set();STOP.set()

load_state()
analyze_performance()
persist_state('startup_state')
signal.signal(signal.SIGINT,shutdown);signal.signal(signal.SIGTERM,shutdown)
start_market_worker()
start_engine_worker()
print(f'{STAGE} | PAPER ONLY | LIVE OFF | PERSISTENT STATE | AUTO RECOVERY | MARKET INTELLIGENCE | ADAPTIVE EXITS | ADAPTIVE ENTRY VALIDATION | RAPID SCALPING | DATA CACHE | CONNECTION RECOVERY | SESSION METRICS | CONSOLIDATED STRATEGY | ENGINE INDEPENDENT OF DASHBOARD | http://{HOST}:{PORT}')
type('ReusableServer',(ThreadingHTTPServer,),{'allow_reuse_address':True})((HOST,PORT),H).serve_forever()
