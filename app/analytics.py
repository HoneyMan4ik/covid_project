import pandas as pd
import numpy as np
from typing import Dict, List
import requests

#dada
def get_covid_data_for_analysis(limit: int = 1000) -> pd.DataFrame:
    try:
        response = requests.get("http://127.0.0.1:8000/covid/aggregate", 
                              params={'date_col': 'DATE', 'value_col': 'CASES', 'limit': limit})
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data['rows'])
            df['date'] = pd.to_datetime(df['date'])
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()


def simple_forecast(df: pd.DataFrame, periods: int = 7) -> pd.DataFrame:
    if df.empty:
        return df
    
    df = df.sort_values('date')
    
    df['ma_7'] = df['value'].rolling(window=7).mean()
    
    last_value = df['value'].iloc[-1]
    trend = (df['value'].iloc[-1] - df['value'].iloc[0]) / len(df)
    
    future_dates = pd.date_range(start=df['date'].max() + pd.Timedelta(days=1), periods=periods)
    future_values = [max(0, last_value + trend * (i + 1)) for i in range(periods)]
    
    return pd.DataFrame({
        'date': future_dates,
        'value': future_values,
        'type': 'forecast'
    })


def basic_patterns(df: pd.DataFrame) -> Dict[str, any]:
    if df.empty:
        return {}
    
    df = df.sort_values('date')
    
    patterns = {
        'total_records': len(df),
        'date_range': {
            'start': df['date'].min().strftime('%Y-%m-%d'),
            'end': df['date'].max().strftime('%Y-%m-%d')
        },
        'total_cases': int(df['value'].sum()),
        'avg_cases_per_day': float(df['value'].mean()),
        'max_cases_in_day': int(df['value'].max()),
        'trend': 'increasing' if df['value'].iloc[-1] > df['value'].iloc[0] else 'decreasing'
    }
    
    return patterns


def get_analytics_summary() -> Dict[str, any]:
    df = get_covid_data_for_analysis()
    
    if df.empty:
        return {"error": "No data available"}
    
    return basic_patterns(df)
