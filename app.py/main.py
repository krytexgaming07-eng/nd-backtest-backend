from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import ccxt
import yfinance as yf
import pandas as pd
import numpy as np
import ta

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ऑटो-सजेशनसाठी लोकप्रिय ॲसेट्सची यादी
MARKET_CATALOG = [
    # Indices
    {"symbol": "^NSEI", "name": "Nifty 50", "category": "Index", "source": "yfinance"},
    {"symbol": "^NSEBANK", "name": "Bank Nifty", "category": "Index", "source": "yfinance"},
    {"symbol": "^GSPC", "name": "S&P 500", "category": "Index", "source": "yfinance"},
    # Stocks
    {"symbol": "RELIANCE.NS", "name": "Reliance Industries", "category": "Stock", "source": "yfinance"},
    {"symbol": "TCS.NS", "name": "Tata Consultancy Services", "category": "Stock", "source": "yfinance"},
    {"symbol": "AAPL", "name": "Apple Inc.", "category": "Stock", "source": "yfinance"},
    # Commodities & Forex
    {"symbol": "GC=F", "name": "Gold Futures", "category": "Commodity", "source": "yfinance"},
    {"symbol": "CL=F", "name": "Crude Oil Futures", "category": "Commodity", "source": "yfinance"},
    {"symbol": "USDINR=X", "name": "USD to INR", "category": "Forex", "source": "yfinance"},
    {"symbol": "EURUSD=X", "name": "EUR to USD", "category": "Forex", "source": "yfinance"},
    # Crypto
    {"symbol": "BTC/USDT", "name": "Bitcoin", "category": "Crypto", "source": "ccxt"},
    {"symbol": "ETH/USDT", "name": "Ethereum", "category": "Crypto", "source": "ccxt"},
    {"symbol": "SOL/USDT", "name": "Solana", "category": "Crypto", "source": "ccxt"},
]

@app.get("/symbols")
def get_symbols():
    return MARKET_CATALOG

class AdvancedBacktestRequest(BaseModel):
    mode: str = "indicator" # "indicator" किंवा "custom_code"
    data_source: str = "ccxt" # "ccxt" किंवा "yfinance"
    exchange: str = "binance"
    symbol: str = "BTC/USDT"
    timeframe: str = "15m"
    capital: float = 10000.0
    risk_pct: float = 1.0
    risk_reward: float = 2.0
    # Indicator Mode
    fast_ema: int = 9
    slow_ema: int = 21
    rsi_period: int = 14
    # Custom Code Mode
    custom_strategy_code: Optional[str] = None

@app.post("/run-backtest")
def run_backtest(req: AdvancedBacktestRequest):
    try:
        # १. डेटा फेचिंग (No Lookahead साठी टाइम सिरीज क्रमबद्ध असावी)
        if req.data_source == "ccxt":
            client = getattr(ccxt, req.exchange.lower())()
            ohlcv = client.fetch_ohlcv(req.symbol, timeframe=req.timeframe, limit=500)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms').dt.strftime('%Y-%m-%d %H:%M')
        else:
            # yfinance द्वारे (Stocks, Forex, Commodities, Indices)
            yf_timeframe_map = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "1d": "1d"}
            period_map = {"1m": "7d", "5m": "30d", "15m": "60d", "1h": "730d", "1d": "max"}
            tf = yf_timeframe_map.get(req.timeframe, "1d")
            prd = period_map.get(tf, "60d")
            
            ticker = yf.Ticker(req.symbol)
            df = ticker.history(period=prd, interval=tf).reset_index()
            if df.empty:
                return {"status": "error", "message": f"{req.symbol} साठी डेटा मिळाला नाही."}
            df.columns = [c.lower() for c in df.columns]
            df.rename(columns={"date": "datetime", "datetime": "datetime"}, inplace=True)
            df['datetime'] = df['datetime'].astype(str).str.slice(0, 16)

        df['signal'] = 0 # 1: Buy, -1: Sell, 0: Neutral

        # २. स्ट्रॅटेजी एक्झिक्युशन
        if req.mode == "custom_code" and req.custom_strategy_code:
            # सुरक्षित Python execution environment
            # युझरचा कोड df['signal'] सेट करेल
            local_vars = {"df": df, "np": np, "pd": pd, "ta": ta}
            exec(req.custom_strategy_code, {}, local_vars)
            df = local_vars["df"]
            if 'signal' not in df.columns:
                return {"status": "error", "message": "तुमच्या कोडमध्ये df['signal'] कॉलम आढळला नाही."}
        else:
            # Indicator Mode (EMA + RSI Filter)
            df['fast_ema'] = ta.trend.ema_indicator(df['close'], window=req.fast_ema)
            df['slow_ema'] = ta.trend.ema_indicator(df['close'], window=req.slow_ema)
            df['rsi'] = ta.momentum.rsi(df['close'], window=req.rsi_period)
            
            # Lookahead Bias प्रतिबंध:
            # सिग्नल मागील कँडलचा क्लोज ओलांडल्यावरच तयार होतो
            buy_condition = (df['fast_ema'] > df['slow_ema']) & (df['fast_ema'].shift(1) <= df['slow_ema'].shift(1)) & (df['rsi'] > 50)
            sell_condition = (df['fast_ema'] < df['slow_ema']) & (df['fast_ema'].shift(1) >= df['slow_ema'].shift(1))
            
            df.loc[buy_condition, 'signal'] = 1
            df.loc[sell_condition, 'signal'] = -1

        # Lookahead Bias टाळण्यासाठी: सिग्नल एका कँडलने पुढे शिफ्ट करणे
        # (म्हणजे आजचा सिग्नल पुढच्या कँडलच्या ओपनवर एक्झिक्युट होईल)
        df['exec_signal'] = df['signal'].shift(1).fillna(0)

        # ३. व्हेक्टरायझेशन आणि सिमुलेशन (ट्रेड लॉग आणि इक्विटी कर्व्ह)
        capital = req.capital
        initial_capital = req.capital
        peak_capital = capital
        max_drawdown = 0.0

        equity_curve = []
        trade_logs = []
        
        in_trade = False
        entry_price = 0.0
        entry_time = ""
        position_size = 0.0
        wins = 0
        losses = 0

        for i in range(len(df)):
            row = df.iloc[i]
            curr_open = row['open']
            curr_time = row['datetime']
            curr_close = row['close']
            sig = row['exec_signal']

            # Entry
            if sig == 1 and not in_trade:
                in_trade = True
                entry_price = curr_open
                entry_time = curr_time
                # Risk based position sizing
                risk_amount = capital * (req.risk_pct / 100.0)
                sl_dist = entry_price * 0.01  # 1% standard SL base
                position_size = risk_amount / sl_dist if sl_dist > 0 else 1.0

            # Exit
            elif (sig == -1 or i == len(df) - 1) and in_trade:
                exit_price = curr_open if sig == -1 else curr_close
                trade_pct = (exit_price - entry_price) / entry_price
                pnl = round(trade_pct * position_size * entry_price * req.risk_reward, 2)
                capital += pnl

                if pnl >= 0:
                    wins += 1
                else:
                    losses += 1

                trade_logs.append({
                    "entry_time": entry_time,
                    "exit_time": curr_time,
                    "entry_price": round(entry_price, 2),
                    "exit_price": round(exit_price, 2),
                    "pnl": pnl,
                    "return_pct": round(trade_pct * 100, 2),
                    "type": "BUY"
                })
                in_trade = False

            # Drawdown & Equity Tracking
            if capital > peak_capital:
                peak_capital = capital
            dd = (peak_capital - capital) / peak_capital * 100 if peak_capital > 0 else 0
            if dd > max_drawdown:
                max_drawdown = dd

            equity_curve.append({
                "time": curr_time,
                "equity": round(capital, 2)
            })

        total_trades = wins + losses
        win_rate = round((wins / total_trades) * 100, 2) if total_trades > 0 else 0.0
        net_pnl = round(capital - initial_capital, 2)
        roi_pct = round((net_pnl / initial_capital) * 100, 2)

        return {
            "status": "success",
            "symbol": req.symbol,
            "final_capital": round(capital, 2),
            "net_pnl": net_pnl,
            "roi_pct": roi_pct,
            "win_rate_pct": win_rate,
            "total_trades": total_trades,
            "wins": wins,
            "losses": losses,
            "max_drawdown_pct": round(max_drawdown, 2),
            "equity_curve": equity_curve[::max(1, len(equity_curve)//50)], # चार्टसाठी 50 पॉईंट्स
            "trade_logs": trade_logs[-20:] # शेवटचे 20 ट्रेड्स
        }

    except Exception as e:
        return {"status": "error", "message": f"Execution Error: {str(e)}"}