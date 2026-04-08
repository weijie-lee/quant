"""
美股数据获取模块
功能：获取股票历史数据
"""

import yfinance as yf
from typing import Optional, Dict, Any
import pandas as pd
from datetime import datetime, timedelta
import time
import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_stock_data(
    ticker: str,
    period: str = "1y",
    interval: str = "1d",
    max_retries: int = 3,
    retry_delay: int = 2
) -> pd.DataFrame:
    """
    获取股票历史数据（带重试机制）
    
    Args:
        ticker: 股票代码，如 "AAPL", "MSFT", "GOOGL"
        period: 数据周期，如 "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"
        interval: 数据间隔，如 "1m", "2m", "5m", "15m", "30m", "60m", "1d", "1wk", "1mo"
        max_retries: 最大重试次数
        retry_delay: 重试延迟（秒）
    
    Returns:
        DataFrame，包含 Open, High, Low, Close, Volume, Adj Close 等列
    
    Example:
        >>> df = get_stock_data("AAPL", period="1y")
        >>> print(df.head())
    """
    for attempt in range(max_retries):
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period=period, interval=interval)
            return df
        except Exception as e:
            if "Rate limit" in str(e) or "429" in str(e):
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)  # 指数退避
                    logger.warning(f"Rate limit hit, retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Rate limit exceeded after {max_retries} attempts")
                    return pd.DataFrame()
            else:
                logger.error(f"Error fetching data: {e}")
                return pd.DataFrame()
    return pd.DataFrame()


def get_stock_info(ticker: str, max_retries: int = 3, retry_delay: int = 2) -> Dict[str, Any]:
    """
    获取股票基本信息（带重试机制）
    
    Args:
        ticker: 股票代码，如 "AAPL"
        max_retries: 最大重试次数
        retry_delay: 重试延迟（秒）
    
    Returns:
        字典，包含公司名、市值、PE 等信息
    
    Example:
        >>> info = get_stock_info("AAPL")
        >>> print(info.get("companyName"))
    """
    for attempt in range(max_retries):
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            return info
        except Exception as e:
            if "Rate limit" in str(e) or "429" in str(e):
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    logger.warning(f"Rate limit hit, retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Rate limit exceeded after {max_retries} attempts")
                    return {}
            else:
                logger.error(f"Error fetching info: {e}")
                return {}
    return {}


def get_stock_price(ticker: str, max_retries: int = 3, retry_delay: int = 2) -> Optional[float]:
    """
    获取股票当前价格（带重试机制）
    
    Args:
        ticker: 股票代码
        max_retries: 最大重试次数
        retry_delay: 重试延迟（秒）
    
    Returns:
        当前价格（float），失败返回 None
    
    Example:
        >>> price = get_stock_price("AAPL")
        >>> print(f"Apple 当前价格: ${price}")
    """
    for attempt in range(max_retries):
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period="1d", interval="1m")
            if df.empty:
                return None
            latest_price = df['Close'].iloc[-1]
            return float(latest_price)
        except Exception as e:
            if "Rate limit" in str(e) or "429" in str(e):
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    logger.warning(f"Rate limit hit, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Rate limit exceeded")
                    return None
            else:
                logger.error(f"Error fetching price: {e}")
                return None
    return None


def get_multiple_stocks(
    tickers: list,
    period: str = "1y",
    interval: str = "1d",
    request_delay: float = 0.5
) -> Dict[str, pd.DataFrame]:
    """
    批量获取多只股票数据（带请求延迟，避免 Rate Limit）
    
    Args:
        tickers: 股票代码列表，如 ["AAPL", "MSFT", "GOOGL"]
        period: 数据周期
        interval: 数据间隔
        request_delay: 请求间隔（秒），避免触发速率限制
    
    Returns:
        字典，key 是股票代码，value 是 DataFrame
    
    Example:
        >>> data = get_multiple_stocks(["AAPL", "MSFT"], period="6mo")
        >>> for ticker, df in data.items():
        ...     print(f"{ticker}: {len(df)} 条数据")
    """
    data = {}
    for ticker in tickers:
        try:
            df = get_stock_data(ticker, period, interval)
            if not df.empty:
                data[ticker] = df
            # 请求间隔，避免触发 Rate Limit
            time.sleep(request_delay)
        except Exception as e:
            logger.error(f"获取 {ticker} 失败: {e}")
    return data


if __name__ == "__main__":
    # 测试：获取苹果公司数据
    print("=" * 50)
    print("测试：获取 AAPL 数据")
    print("=" * 50)
    
    # 获取历史数据
    df = get_stock_data("AAPL", period="1mo")
    print(f"\n历史数据 (最近1个月):")
    print(df.tail())
    
    # 获取基本信息
    info = get_stock_info("AAPL")
    print(f"\n公司名称: {info.get('companyName', 'N/A')}")
    print(f"当前价格: ${info.get('currentPrice', 'N/A')}")
    if info.get('marketCap'):
        print(f"市值: ${info.get('marketCap'):,.0f}")
    print(f"PE 比率: {info.get('trailingPE', 'N/A')}")
    
    # 获取实时价格
    price = get_stock_price("AAPL")
    print(f"\n实时价格: ${price}")
