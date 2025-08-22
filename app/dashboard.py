import dash
from dash import dcc, html, Input, Output
import plotly.graph_objs as go
import requests
import pandas as pd
import os


app = dash.Dash(__name__, title="COVID-19 Dashboard")
API_BASE = os.getenv("API_BASE", "http://127.0.0.1:8000")


app.layout = html.Div([
    html.H1("COVID Dashboard", style={'textAlign': 'center'}),

    html.Div([
        html.Label("Value Column"),
        dcc.Dropdown(
            id='value-col',
            options=[{'label': c, 'value': c} for c in ['CASES','DEATHS','TOTAL_TESTS','POSITIVE_TESTS','REPORTED_CASES','REPORTED_DEATHS','REPORTED_TESTS']],
            value='CASES',
            style={'width': '300px'}
        )
    ], style={'display': 'flex', 'gap': '12px', 'justifyContent': 'center', 'marginBottom': '16px'}),

    dcc.Graph(id='cases-chart'),
    dcc.Graph(id='mobility-chart'),

    html.Div([
        html.H4("Mobility Snapshot & Correlation", style={'marginTop': '12px'}),
        html.Div(id='mobility-summary', style={'marginBottom': '8px', 'color': '#2c3e50'}),
        html.Div(id='mobility-table', style={'overflowX': 'auto'})
    ], style={'padding': '0 16px'})
])


@app.callback(
    [
        Output('cases-chart', 'figure'),
        Output('mobility-chart', 'figure'),
        Output('mobility-summary', 'children'),
        Output('mobility-table', 'children'),
    ],
    [Input('value-col', 'value')]
)
def render_chart(value_col: str):
    try:
        ts_resp = requests.get(
            f"{API_BASE}/covid/aggregate",
            params={'date_col': 'DATE', 'value_col': value_col, 'limit': 20000},
            timeout=8,
        )
        ts_resp.raise_for_status()
        ts = ts_resp.json()
        ts_df = pd.DataFrame(ts.get('rows', []))
        if not ts_df.empty:
            ts_df['date'] = pd.to_datetime(ts_df['date'])
            ts_df = ts_df.sort_values('date')

        mob_resp = requests.get(f"{API_BASE}/eda/mobility", timeout=8)
        mob_resp.raise_for_status()
        mob = mob_resp.json()
        mob_df = pd.DataFrame(mob.get('rows', []))

        cases_fig = go.Figure()
        if not ts_df.empty:
            cases_fig.add_trace(go.Scatter(
                x=ts_df['date'], y=ts_df['value'], mode='lines', name=value_col, line=dict(color='#e74c3c')
            ))
        cases_fig.update_layout(title=f"{value_col} Over Time", xaxis_title='Date', yaxis_title=value_col, hovermode='x unified')

        mobility_fig = go.Figure()
        if not mob_df.empty and 'MONTH' in mob_df.columns:
            mob_df['MONTH'] = pd.to_datetime(mob_df['MONTH'])
            for col, name, color in [
                ('RETAIL', 'Retail & Recreation', '#3498db'),
                ('WORKPLACES', 'Workplaces', '#2ecc71'),
                ('RESIDENTIAL', 'Residential', '#f39c12'),
            ]:
                if col in mob_df.columns:
                    mobility_fig.add_trace(go.Scatter(
                        x=mob_df['MONTH'], y=mob_df[col], mode='lines', name=name, line=dict(dash='dot', color=color)
                    ))
        mobility_fig.update_layout(title="Mobility Trends (%)", xaxis_title='Month', yaxis_title='Change (%)', hovermode='x unified')

        summary = html.Span("")
        table_comp = html.Span("")

        try:
            if not ts_df.empty and not mob_df.empty:
                ts_m = ts_df.set_index('date').resample('MS').sum(numeric_only=True).reset_index()
                ts_m = ts_m.rename(columns={'date': 'MONTH', 'value': 'MONTHLY_VALUE'})
                ts_m['MONTH'] = pd.to_datetime(ts_m['MONTH'])
                merged = pd.merge(ts_m, mob_df, on='MONTH', how='left')

                mobility_cols = [c for c in ['RETAIL','WORKPLACES','RESIDENTIAL'] if c in merged.columns]
                if mobility_cols:
                    has_mob = merged[mobility_cols].notna().any(axis=1)
                    merged = merged[has_mob]

                corr_parts = []
                for col, label in [('RETAIL','Retail'), ('WORKPLACES','Workplaces'), ('RESIDENTIAL','Residential')]:
                    if col in merged.columns:
                        c = merged[['MONTHLY_VALUE', col]].dropna().corr().iloc[0,1]
                        if pd.notnull(c):
                            corr_parts.append(f"{label}: {c:.2f}")
                summary_text = " | ".join(corr_parts) or "No correlation available"
                summary = html.Div([html.Strong("Correlation (monthly value vs mobility): "), summary_text])

                keep_cols = ['MONTH','MONTHLY_VALUE'] + mobility_cols
                view = merged[keep_cols].copy()
                for col in ['RETAIL','WORKPLACES','RESIDENTIAL']:
                    if col in view.columns:
                        view[col] = view[col].map(lambda v: None if pd.isna(v) else round(float(v),1))
                view = view.tail(12)
                rows = []
                for _, r in view.iterrows():
                    rows.append(html.Tr([
                        html.Td(r['MONTH'].strftime('%Y-%m')),
                        html.Td(f"{int(r['MONTHLY_VALUE']):,}" if pd.notna(r['MONTHLY_VALUE']) else 'N/A'),
                        html.Td('N/A' if 'RETAIL' not in r or pd.isna(r.get('RETAIL')) else f"{r['RETAIL']:.1f}%"),
                        html.Td('N/A' if 'WORKPLACES' not in r or pd.isna(r.get('WORKPLACES')) else f"{r['WORKPLACES']:.1f}%"),
                        html.Td('N/A' if 'RESIDENTIAL' not in r or pd.isna(r.get('RESIDENTIAL')) else f"{r['RESIDENTIAL']:.1f}%"),
                    ]))
                table_comp = html.Table([
                    html.Thead(html.Tr([
                        html.Th('Month', style={'padding':'6px','borderBottom':'1px solid #ddd'}),
                        html.Th(value_col, style={'padding':'6px','borderBottom':'1px solid #ddd'}),
                        html.Th('Retail', style={'padding':'6px','borderBottom':'1px solid #ddd'}),
                        html.Th('Workplaces', style={'padding':'6px','borderBottom':'1px solid #ddd'}),
                        html.Th('Residential', style={'padding':'6px','borderBottom':'1px solid #ddd'}),
                    ])),
                    html.Tbody(rows)
                ], style={'width': '100%', 'borderCollapse': 'collapse'})
        except Exception:
            pass

        return cases_fig, mobility_fig, summary, table_comp
    except requests.exceptions.RequestException as e:
        f1, f2 = go.Figure(), go.Figure()
        f1.add_annotation(text=f"API not reachable at {API_BASE}. Start API or set API_BASE.", showarrow=False)
        f2.add_annotation(text=f"API not reachable at {API_BASE}. Start API or set API_BASE.", showarrow=False)
        return f1, f2, "", ""
    except Exception as e:
        f1, f2 = go.Figure(), go.Figure()
        f1.add_annotation(text=f"Error: {e}", showarrow=False)
        f2.add_annotation(text=f"Error: {e}", showarrow=False)
        return f1, f2, "", ""


if __name__ == '__main__':
    app.run_server(debug=True, port=8051)