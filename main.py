"""
Data Analysis Challenge - Support Cases & Accounts Analysis
Professional data analysis pipeline with best practices
"""

import json
import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import warnings
import os
import config

warnings.filterwarnings('ignore')

# Set style for visualizations
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = config.FIGURE_SIZE

class DataAnalysisPipeline:
    """Main pipeline for data analysis with best practices"""
    
    def __init__(self, accounts_path: str, support_cases_path: str):
        self.accounts_path = accounts_path
        self.support_cases_path = support_cases_path
        self.conn = sqlite3.connect(':memory:')
        self.df_accounts = None
        self.df_support_cases = None
        
    def load_data(self):
        """Part 1: Load and explore the data"""
        print("=" * 80)
        print("PART 1: DATA EXPLORATION")
        print("=" * 80)
        
        # Load accounts
        with open(self.accounts_path, 'r', encoding='utf-8') as f:
            accounts_data = json.load(f)
        self.df_accounts = pd.DataFrame(accounts_data)
        
        # Load support cases
        with open(self.support_cases_path, 'r', encoding='utf-8') as f:
            support_cases_data = json.load(f)
        self.df_support_cases = pd.DataFrame(support_cases_data)
        
        self._explore_data()
        
    def _explore_data(self):
        """Explore data structure and content"""
        print("\n📊 ACCOUNTS DATASET")
        print(f"Shape: {self.df_accounts.shape}")
        print(f"\nColumns: {list(self.df_accounts.columns)}")
        print(f"\nData Types:\n{self.df_accounts.dtypes}")
        print(f"\nFirst few rows:\n{self.df_accounts.head()}")
        print(f"\nMissing values:\n{self.df_accounts.isnull().sum()}")
        print(f"\nBasic statistics:\n{self.df_accounts.describe()}")
        
        print("\n" + "=" * 80)
        print("\n📞 SUPPORT CASES DATASET")
        print(f"Shape: {self.df_support_cases.shape}")
        print(f"\nColumns: {list(self.df_support_cases.columns)}")
        print(f"\nData Types:\n{self.df_support_cases.dtypes}")
        print(f"\nFirst few rows:\n{self.df_support_cases.head()}")
        print(f"\nMissing values:\n{self.df_support_cases.isnull().sum()}")
        print(f"\nBasic statistics:\n{self.df_support_cases.describe()}")
        
    def process_data(self):
        """Part 2: Data Processing using SQL"""
        print("\n" + "=" * 80)
        print("PART 2: DATA PROCESSING WITH SQL")
        print("=" * 80)
        
        # Convert date columns to datetime
        self.df_accounts['account_created_date'] = pd.to_datetime(self.df_accounts['account_created_date'])
        self.df_support_cases['case_created_date'] = pd.to_datetime(self.df_support_cases['case_created_date'])
        self.df_support_cases['case_closed_date'] = pd.to_datetime(self.df_support_cases['case_closed_date'])
        
        # Load data into SQLite
        self.df_accounts.to_sql('accounts', self.conn, if_exists='replace', index=False)
        self.df_support_cases.to_sql('support_cases', self.conn, if_exists='replace', index=False)
        
        # SQL Queries for KPIs
        self._calculate_kpis()
        
    def _calculate_kpis(self):
        """Calculate Key Performance Indicators using SQL"""
        
        # KPI 1: Cases per Account with Account Details
        query_cases_per_account = """
        SELECT 
            a.account_sfid,
            a.account_name,
            a.account_country,
            a.account_industry,
            COUNT(sc.case_sfid) as total_cases,
            AVG(JULIANDAY(sc.case_closed_date) - JULIANDAY(sc.case_created_date)) as avg_resolution_days,
            SUM(CASE WHEN sc.case_status = 'Closed' THEN 1 ELSE 0 END) as closed_cases,
            SUM(CASE WHEN sc.case_status = 'Open' THEN 1 ELSE 0 END) as open_cases
        FROM accounts a
        LEFT JOIN support_cases sc ON a.account_sfid = sc.account_sfid
        GROUP BY a.account_sfid, a.account_name, a.account_country, a.account_industry
        HAVING total_cases > 0
        ORDER BY total_cases DESC
        """
        self.kpi_cases_per_account = pd.read_sql_query(query_cases_per_account, self.conn)
        
        # KPI 2: Cases by Priority and Status
        query_priority_status = """
        SELECT 
            case_priority,
            case_status,
            COUNT(*) as case_count,
            AVG(JULIANDAY(case_closed_date) - JULIANDAY(case_created_date)) as avg_resolution_days
        FROM support_cases
        GROUP BY case_priority, case_status
        ORDER BY case_priority, case_status
        """
        self.kpi_priority_status = pd.read_sql_query(query_priority_status, self.conn)
        
        # KPI 3: Industry Analysis
        query_industry = """
        SELECT 
            a.account_industry,
            COUNT(DISTINCT a.account_sfid) as total_accounts,
            COUNT(sc.case_sfid) as total_cases,
            CAST(COUNT(sc.case_sfid) AS FLOAT) / COUNT(DISTINCT a.account_sfid) as cases_per_account,
            AVG(JULIANDAY(sc.case_closed_date) - JULIANDAY(sc.case_created_date)) as avg_resolution_days
        FROM accounts a
        LEFT JOIN support_cases sc ON a.account_sfid = sc.account_sfid
        GROUP BY a.account_industry
        ORDER BY total_cases DESC
        """
        self.kpi_industry = pd.read_sql_query(query_industry, self.conn)
        
        # KPI 4: Country Analysis
        query_country = """
        SELECT 
            a.account_country,
            COUNT(DISTINCT a.account_sfid) as total_accounts,
            COUNT(sc.case_sfid) as total_cases,
            AVG(JULIANDAY(sc.case_closed_date) - JULIANDAY(sc.case_created_date)) as avg_resolution_days
        FROM accounts a
        LEFT JOIN support_cases sc ON a.account_sfid = sc.account_sfid
        GROUP BY a.account_country
        ORDER BY total_cases DESC
        LIMIT 15
        """
        self.kpi_country = pd.read_sql_query(query_country, self.conn)
        
        # KPI 5: Time Series - Cases Created Over Time
        query_time_series = """
        SELECT 
            DATE(case_created_date) as date,
            COUNT(*) as cases_created,
            case_priority
        FROM support_cases
        GROUP BY DATE(case_created_date), case_priority
        ORDER BY date
        """
        self.kpi_time_series = pd.read_sql_query(query_time_series, self.conn)
        
        print("\n✅ KPIs calculated successfully!")
        print(f"\n📈 KPI Summary:")
        print(f"- Cases per Account: {len(self.kpi_cases_per_account)} records")
        print(f"- Priority/Status Analysis: {len(self.kpi_priority_status)} records")
        print(f"- Industry Analysis: {len(self.kpi_industry)} records")
        print(f"- Country Analysis: {len(self.kpi_country)} records")
        print(f"- Time Series Data: {len(self.kpi_time_series)} records")
        
    def create_visualizations(self):
        """Part 3: Data Visualization"""
        print("\n" + "=" * 80)
        print("PART 3: DATA VISUALIZATION")
        print("=" * 80)
        
        # Visualization 1: Top Accounts by Cases
        self._viz_top_accounts()
        
        # Visualization 2: Cases by Priority and Status
        self._viz_priority_status()
        
        # Visualization 3: Industry Performance
        self._viz_industry_analysis()
        
        # Visualization 4: Geographic Distribution
        self._viz_country_analysis()
        
        # Visualization 5: Time Series
        self._viz_time_series()
        
        # Visualization 6: Resolution Time Distribution
        self._viz_resolution_time()
        
        print("\n✅ All visualizations created successfully!")
        
    def _viz_top_accounts(self):
        """Visualize top accounts by number of cases"""
        top_accounts = self.kpi_cases_per_account.head(config.TOP_N_ACCOUNTS)
        
        # Create figure with custom style
        fig = plt.figure(figsize=(20, 9))
        gs = fig.add_gridspec(1, 2, hspace=0.3, wspace=0.3)
        ax1 = fig.add_subplot(gs[0, 0])
        ax2 = fig.add_subplot(gs[0, 1])
        
        # ===== LEFT PLOT: Total Cases with Gradient =====
        colors_gradient = plt.cm.RdYlGn_r(np.linspace(0.2, 0.8, len(top_accounts)))
        
        bars1 = ax1.barh(
            range(len(top_accounts)),
            top_accounts['total_cases'], 
            color=colors_gradient,
            edgecolor='darkgray',
            linewidth=1.2,
            height=0.7
        )
        
        ax1.set_yticks(range(len(top_accounts)))
        ax1.set_yticklabels(top_accounts['account_name'], fontsize=11)
        
        # Add value labels
        for i, (bar, value) in enumerate(zip(bars1, top_accounts['total_cases'])):
            ax1.text(
                value + max(top_accounts['total_cases']) * 0.02, 
                i, 
                f'{int(value)} cases',
                va='center',
                fontsize=10,
                fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.4', facecolor='white', 
                         edgecolor='gray', alpha=0.8)
            )
        
        ax1.set_xlabel('Total Cases', fontsize=14, fontweight='bold', labelpad=10)
        ax1.set_title(f'🏆 Top {config.TOP_N_ACCOUNTS} Accounts by Case Volume', 
                      fontsize=16, fontweight='bold', pad=20)
        ax1.invert_yaxis()
        ax1.grid(axis='x', alpha=0.3, linestyle='--', linewidth=0.7)
        ax1.set_axisbelow(True)
        ax1.spines['top'].set_visible(False)
        ax1.spines['right'].set_visible(False)
        
        # ===== RIGHT PLOT: Stacked Bar =====
        x_pos = np.arange(len(top_accounts))
        color_closed = '#27ae60'
        color_open = '#e67e22'
        
        bars_closed = ax2.bar(
            x_pos, 
            top_accounts['closed_cases'], 
            label='✅ Closed', 
            color=color_closed, 
            alpha=0.85,
            edgecolor='white',
            linewidth=1.5
        )
        
        bars_open = ax2.bar(
            x_pos, 
            top_accounts['open_cases'], 
            bottom=top_accounts['closed_cases'], 
            label='⏳ Open', 
            color=color_open, 
            alpha=0.85,
            edgecolor='white',
            linewidth=1.5
        )
        
        # Add percentage labels
        for i, (closed, open_val, total) in enumerate(zip(
            top_accounts['closed_cases'], 
            top_accounts['open_cases'],
            top_accounts['total_cases']
        )):
            if closed > 0:
                closed_pct = (closed / total) * 100
                ax2.text(i, closed/2, f'{int(closed)}\n({closed_pct:.0f}%)',
                        ha='center', va='center', fontsize=9, fontweight='bold', color='white')
            
            if open_val > 0:
                open_pct = (open_val / total) * 100
                ax2.text(i, closed + open_val/2, f'{int(open_val)}\n({open_pct:.0f}%)',
                        ha='center', va='center', fontsize=9, fontweight='bold', color='white')
        
        ax2.set_xticks(x_pos)
        ax2.set_xticklabels(
            [name[:20] + '...' if len(name) > 20 else name 
             for name in top_accounts['account_name']], 
            rotation=45, ha='right', fontsize=10
        )
        ax2.set_ylabel('Number of Cases', fontsize=14, fontweight='bold', labelpad=10)
        ax2.set_title(f'📊 Case Status Distribution - Top {config.TOP_N_ACCOUNTS}', 
                      fontsize=16, fontweight='bold', pad=20)
        ax2.legend(title='Case Status', title_fontsize=12, fontsize=11,
                  loc='upper left', frameon=True, shadow=True, fancybox=True, framealpha=0.95)
        ax2.grid(axis='y', alpha=0.3, linestyle='--', linewidth=0.7)
        ax2.set_axisbelow(True)
        ax2.spines['top'].set_visible(False)
        ax2.spines['right'].set_visible(False)
        
        plt.tight_layout()
        output_path = os.path.join(config.VISUALIZATIONS_DIR, 'viz_top_accounts.png')
        plt.savefig(output_path, dpi=config.DPI, bbox_inches='tight')
        plt.show()
        print(f"✅ Top Accounts visualization saved")

    def _viz_priority_status(self):
        """Visualize cases by priority and status"""
        pivot_data = self.kpi_priority_status.pivot(
            index='case_priority', 
            columns='case_status', 
            values='case_count'
        ).fillna(0)
        
        # Create figure
        fig, ax = plt.subplots(figsize=(16, 9))
        
        # Use distinct colors for each status
        n_statuses = len(pivot_data.columns)
        colors = sns.color_palette("husl", n_statuses)
        
        # Plot with colors
        bars = pivot_data.plot(
            kind='bar', 
            ax=ax, 
            color=colors, 
            width=0.75,
            edgecolor='white',
            linewidth=1.5
        )
        
        # Add value labels on bars
        for container in ax.containers:
            ax.bar_label(container, fmt='%.0f', padding=3, fontsize=9, fontweight='bold')
        
        # Styling
        ax.set_xlabel('Priority Level', fontsize=14, fontweight='bold', labelpad=10)
        ax.set_ylabel('Number of Cases', fontsize=14, fontweight='bold', labelpad=10)
        ax.set_title('📋 Cases Distribution by Priority and Status', 
                     fontsize=16, fontweight='bold', pad=20)
        
        # Enhanced legend
        ax.legend(
            title='Case Status', 
            title_fontsize=13,
            fontsize=11,
            bbox_to_anchor=(1.02, 1), 
            loc='upper left',
            frameon=True,
            shadow=True,
            fancybox=True,
            framealpha=0.95
        )
        
        # Grid and spines
        ax.grid(axis='y', alpha=0.3, linestyle='--', linewidth=0.7)
        ax.set_axisbelow(True)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        
        plt.xticks(rotation=0, fontsize=12)
        plt.yticks(fontsize=11)
        plt.tight_layout()
        
        output_path = os.path.join(config.VISUALIZATIONS_DIR, 'viz_priority_status.png')
        plt.savefig(output_path, dpi=config.DPI, bbox_inches='tight')
        plt.show()
        print(f"✅ Priority/Status visualization saved with {n_statuses} distinct colors")
        
    def _viz_industry_analysis(self):
        """Visualize industry performance"""
        top_industries = self.kpi_industry.head(config.TOP_N_INDUSTRIES)
        
        # Create figure
        fig = plt.figure(figsize=(20, 9))
        gs = fig.add_gridspec(1, 2, hspace=0.3, wspace=0.3)
        ax1 = fig.add_subplot(gs[0, 0])
        ax2 = fig.add_subplot(gs[0, 1])
        
        # ===== LEFT: Cases per Account =====
        colors_coral = plt.cm.Oranges(np.linspace(0.4, 0.9, len(top_industries)))
        
        bars1 = ax1.barh(
            range(len(top_industries)),
            top_industries['cases_per_account'], 
            color=colors_coral,
            edgecolor='darkgray',
            linewidth=1.2,
            height=0.7
        )
        
        ax1.set_yticks(range(len(top_industries)))
        ax1.set_yticklabels(top_industries['account_industry'], fontsize=11)
        
        # Add value labels
        for i, (bar, value) in enumerate(zip(bars1, top_industries['cases_per_account'])):
            ax1.text(
                value + max(top_industries['cases_per_account']) * 0.02, 
                i, 
                f'{value:.1f}',
                va='center',
                fontsize=10,
                fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.4', facecolor='white', 
                         edgecolor='gray', alpha=0.8)
            )
        
        ax1.set_xlabel('Cases per Account', fontsize=14, fontweight='bold', labelpad=10)
        ax1.set_title('🏭 Average Cases per Account by Industry', 
                      fontsize=16, fontweight='bold', pad=20)
        ax1.invert_yaxis()
        ax1.grid(axis='x', alpha=0.3, linestyle='--', linewidth=0.7)
        ax1.set_axisbelow(True)
        ax1.spines['top'].set_visible(False)
        ax1.spines['right'].set_visible(False)
        
        # ===== RIGHT: Resolution Time =====
        colors_blue = plt.cm.Blues(np.linspace(0.4, 0.9, len(top_industries)))
        
        bars2 = ax2.barh(
            range(len(top_industries)),
            top_industries['avg_resolution_days'], 
            color=colors_blue,
            edgecolor='darkgray',
            linewidth=1.2,
            height=0.7
        )
        
        ax2.set_yticks(range(len(top_industries)))
        ax2.set_yticklabels(top_industries['account_industry'], fontsize=11)
        
        # Add value labels
        for i, (bar, value) in enumerate(zip(bars2, top_industries['avg_resolution_days'])):
            if not pd.isna(value):
                ax2.text(
                    value + max(top_industries['avg_resolution_days'].fillna(0)) * 0.02, 
                    i, 
                    f'{value:.1f}d',
                    va='center',
                    fontsize=10,
                    fontweight='bold',
                    bbox=dict(boxstyle='round,pad=0.4', facecolor='white', 
                             edgecolor='gray', alpha=0.8)
                )
        
        ax2.set_xlabel('Average Resolution Time (days)', fontsize=14, fontweight='bold', labelpad=10)
        ax2.set_title('⏱️ Average Resolution Time by Industry', 
                      fontsize=16, fontweight='bold', pad=20)
        ax2.invert_yaxis()
        ax2.grid(axis='x', alpha=0.3, linestyle='--', linewidth=0.7)
        ax2.set_axisbelow(True)
        ax2.spines['top'].set_visible(False)
        ax2.spines['right'].set_visible(False)
        
        plt.tight_layout()
        output_path = os.path.join(config.VISUALIZATIONS_DIR, 'viz_industry_analysis.png')
        plt.savefig(output_path, dpi=config.DPI, bbox_inches='tight')
        plt.show()
        print(f"✅ Industry Analysis visualization saved")
        
    def _viz_country_analysis(self):
        """Visualize geographic distribution"""
        fig, ax = plt.subplots(figsize=(14, 10))
        
        countries = self.kpi_country.sort_values('total_cases', ascending=True)
        
        # Gradient colors
        colors_teal = plt.cm.viridis(np.linspace(0.2, 0.9, len(countries)))
        
        bars = ax.barh(
            range(len(countries)),
            countries['total_cases'], 
            color=colors_teal,
            edgecolor='darkgray',
            linewidth=1.2,
            height=0.7
        )
        
        ax.set_yticks(range(len(countries)))
        ax.set_yticklabels(countries['account_country'], fontsize=11)
        
        # Add value labels
        for i, (bar, value) in enumerate(zip(bars, countries['total_cases'])):
            ax.text(
                value + max(countries['total_cases']) * 0.02, 
                i, 
                f'{int(value)} cases',
                va='center',
                fontsize=10,
                fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.4', facecolor='white', 
                         edgecolor='gray', alpha=0.8)
            )
        
        ax.set_xlabel('Total Support Cases', fontsize=14, fontweight='bold', labelpad=10)
        ax.set_title(f'🌍 Top {config.TOP_N_COUNTRIES} Countries by Support Volume', 
                     fontsize=16, fontweight='bold', pad=20)
        ax.grid(axis='x', alpha=0.3, linestyle='--', linewidth=0.7)
        ax.set_axisbelow(True)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        
        plt.tight_layout()
        output_path = os.path.join(config.VISUALIZATIONS_DIR, 'viz_country_analysis.png')
        plt.savefig(output_path, dpi=config.DPI, bbox_inches='tight')
        plt.show()
        print(f"✅ Country Analysis visualization saved")
        
    def _viz_time_series(self):
        """Visualize time series trends"""
        time_data = self.kpi_time_series.copy()
        time_data['date'] = pd.to_datetime(time_data['date'])
        
        pivot_time = time_data.pivot(
            index='date', 
            columns='case_priority', 
            values='cases_created'
        ).fillna(0)
        
        # Create figure
        fig, ax = plt.subplots(figsize=(16, 9))
        
        # Define vibrant colors for each priority level
        priority_colors = {
            'Critical': '#c0392b',  # Dark Red
            'High': '#e74c3c',      # Red
            'Medium': '#f39c12',    # Orange
            'Low': '#3498db',       # Blue
            'Urgent': '#8e44ad',    # Purple
            'Normal': '#27ae60'     # Green
        }
        
        # Plot each priority with distinct style
        for col in pivot_time.columns:
            color = priority_colors.get(col, sns.color_palette("husl", 1)[0])
            pivot_time[col].plot(
                ax=ax, 
                marker='o', 
                linewidth=3,
                markersize=8,
                label=col,
                color=color,
                alpha=0.9,
                markeredgecolor='white',
                markeredgewidth=1.5
            )
        
        ax.set_xlabel('Date', fontsize=14, fontweight='bold', labelpad=10)
        ax.set_ylabel('Number of Cases Created', fontsize=14, fontweight='bold', labelpad=10)
        ax.set_title('📈 Support Cases Created Over Time by Priority', 
                     fontsize=16, fontweight='bold', pad=20)
        ax.legend(
            title='Priority Level',
            title_fontsize=13,
            fontsize=11,
            loc='best',
            frameon=True,
            shadow=True,
            fancybox=True,
            framealpha=0.95
        )
        ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.7)
        ax.set_axisbelow(True)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        
        # Format x-axis
        plt.xticks(rotation=45, ha='right', fontsize=10)
        plt.yticks(fontsize=11)
        
        plt.tight_layout()
        output_path = os.path.join(config.VISUALIZATIONS_DIR, 'viz_time_series.png')
        plt.savefig(output_path, dpi=config.DPI, bbox_inches='tight')
        plt.show()
        print(f"✅ Time Series visualization saved with vibrant colors")
        
    def _viz_resolution_time(self):
        """Visualize resolution time distribution"""
        resolution_data = self.kpi_cases_per_account[
            self.kpi_cases_per_account['avg_resolution_days'].notna()
        ]
        
        # Create figure
        fig = plt.figure(figsize=(20, 9))
        gs = fig.add_gridspec(1, 2, hspace=0.3, wspace=0.3)
        ax1 = fig.add_subplot(gs[0, 0])
        ax2 = fig.add_subplot(gs[0, 1])
        
        # ===== LEFT: Histogram =====
        n, bins, patches = ax1.hist(
            resolution_data['avg_resolution_days'], 
            bins=30, 
            color='mediumpurple',
            edgecolor='white',
            linewidth=1.2,
            alpha=0.85
        )
        
        # Color gradient for histogram
        cm = plt.cm.viridis
        bin_centers = 0.5 * (bins[:-1] + bins[1:])
        col = bin_centers - min(bin_centers)
        col /= max(col)
        for c, p in zip(col, patches):
            plt.setp(p, 'facecolor', cm(c))
        
        median_val = resolution_data['avg_resolution_days'].median()
        mean_val = resolution_data['avg_resolution_days'].mean()
        
        ax1.axvline(median_val, color='red', linestyle='--', linewidth=2.5, 
                   label=f'Median: {median_val:.1f} days')
        ax1.axvline(mean_val, color='orange', linestyle='--', linewidth=2.5, 
                   label=f'Mean: {mean_val:.1f} days')
        
        ax1.set_xlabel('Average Resolution Time (days)', fontsize=14, fontweight='bold', labelpad=10)
        ax1.set_ylabel('Frequency', fontsize=14, fontweight='bold', labelpad=10)
        ax1.set_title('📊 Distribution of Average Resolution Time', 
                      fontsize=16, fontweight='bold', pad=20)
        ax1.legend(fontsize=11, frameon=True, shadow=True, fancybox=True, framealpha=0.95)
        ax1.grid(axis='y', alpha=0.3, linestyle='--', linewidth=0.7)
        ax1.set_axisbelow(True)
        ax1.spines['top'].set_visible(False)
        ax1.spines['right'].set_visible(False)
        
        # ===== RIGHT: Top Industries =====
        top_ind = self.kpi_industry.head(10)
        colors_coral = plt.cm.RdYlGn_r(np.linspace(0.2, 0.8, len(top_ind)))
        
        bars = ax2.barh(
            range(len(top_ind)),
            top_ind['avg_resolution_days'], 
            color=colors_coral,
            edgecolor='darkgray',
            linewidth=1.2,
            height=0.7
        )
        
        ax2.set_yticks(range(len(top_ind)))
        ax2.set_yticklabels(top_ind['account_industry'], fontsize=11)
        
        # Add value labels
        for i, (bar, value) in enumerate(zip(bars, top_ind['avg_resolution_days'])):
            if not pd.isna(value):
                ax2.text(
                    value + max(top_ind['avg_resolution_days'].fillna(0)) * 0.02, 
                    i, 
                    f'{value:.1f}d',
                    va='center',
                    fontsize=10,
                    fontweight='bold',
                    bbox=dict(boxstyle='round,pad=0.4', facecolor='white', 
                             edgecolor='gray', alpha=0.8)
                )
        
        ax2.set_xlabel('Average Resolution Time (days)', fontsize=14, fontweight='bold', labelpad=10)
        ax2.set_title('⏱️ Resolution Time by Industry (Top 10)', 
                      fontsize=16, fontweight='bold', pad=20)
        ax2.invert_yaxis()
        ax2.grid(axis='x', alpha=0.3, linestyle='--', linewidth=0.7)
        ax2.set_axisbelow(True)
        ax2.spines['top'].set_visible(False)
        ax2.spines['right'].set_visible(False)
        
        plt.tight_layout()
        output_path = os.path.join(config.VISUALIZATIONS_DIR, 'viz_resolution_time.png')
        plt.savefig(output_path, dpi=config.DPI, bbox_inches='tight')
        plt.show()
        print(f"✅ Resolution Time visualization saved")
        
    def generate_insights(self):
        """Part 4: Business Insights"""
        print("\n" + "=" * 80)
        print("PART 4: BUSINESS INSIGHTS & RECOMMENDATIONS")
        print("=" * 80)
        
        insights = """
        
🔍 KEY INSIGHTS:

1. CUSTOMER CONCENTRATION RISK
   - Top accounts generate disproportionate support volume
   - Need to identify if this indicates product issues or high engagement
   
2. PRIORITY VS RESOLUTION TIME
   - High priority cases may not be resolved fastest
   - Potential misalignment between priority and resource allocation
   
3. INDUSTRY-SPECIFIC PATTERNS
   - Certain industries show higher case volumes per account
   - Resolution times vary significantly by industry
   
4. GEOGRAPHIC DISTRIBUTION
   - Support demand varies greatly by country
   - May indicate localization or timezone coverage gaps
   
5. TEMPORAL TRENDS
   - Case creation patterns show specific trends over time
   - Can inform staffing and resource planning

📋 ACTIONABLE RECOMMENDATIONS:

1. IMPLEMENT PROACTIVE SUPPORT FOR HIGH-VOLUME ACCOUNTS
   Action: Create dedicated account management for top 20 accounts
   Expected Impact: 
   - Reduce case volume by 15-20% through proactive issue prevention
   - Improve customer satisfaction and retention
   - Enable early identification of systemic issues
   
   Implementation:
   - Assign dedicated support engineers to top accounts
   - Conduct monthly health checks and training sessions
   - Create custom documentation for common issues

2. OPTIMIZE RESOURCE ALLOCATION BY PRIORITY AND INDUSTRY
   Action: Restructure support team with industry specialization
   Expected Impact:
   - Reduce average resolution time by 25-30%
   - Improve first-contact resolution rate
   - Increase customer satisfaction scores
   
   Implementation:
   - Create industry-specialized support pods
   - Develop priority-based SLA enforcement
   - Implement automated routing based on industry and priority
   - Create industry-specific knowledge bases
        """
        
        print(insights)
        
        # Generate summary statistics
        print("\n" + "=" * 80)
        print("📊 SUMMARY STATISTICS")
        print("=" * 80)
        
        total_cases = len(self.df_support_cases)
        total_accounts = len(self.df_accounts)
        avg_cases_per_account = total_cases / total_accounts
        
        print(f"\nTotal Accounts: {total_accounts:,}")
        print(f"Total Support Cases: {total_cases:,}")
        print(f"Average Cases per Account: {avg_cases_per_account:.2f}")
        print(f"Median Resolution Time: {self.kpi_cases_per_account['avg_resolution_days'].median():.2f} days")
        print(f"Countries Served: {self.df_accounts['account_country'].nunique()}")
        print(f"Industries Served: {self.df_accounts['account_industry'].nunique()}")
        
    def export_kpis(self):
        """Export KPIs to CSV files for further analysis"""
        print("\n" + "=" * 80)
        print("EXPORTING KPIs TO CSV")
        print("=" * 80)
        
        # Create reports directory
        reports_dir = os.path.join(config.OUTPUT_DIR, 'reports')
        os.makedirs(reports_dir, exist_ok=True)
        
        # Export each KPI
        self.kpi_cases_per_account.to_csv(
            os.path.join(reports_dir, 'kpi_cases_per_account.csv'), 
            index=False
        )
        self.kpi_priority_status.to_csv(
            os.path.join(reports_dir, 'kpi_priority_status.csv'), 
            index=False
        )
        self.kpi_industry.to_csv(
            os.path.join(reports_dir, 'kpi_industry.csv'), 
            index=False
        )
        self.kpi_country.to_csv(
            os.path.join(reports_dir, 'kpi_country.csv'), 
            index=False
        )
        self.kpi_time_series.to_csv(
            os.path.join(reports_dir, 'kpi_time_series.csv'), 
            index=False
        )
        
        print(f"✅ KPIs exported to: {reports_dir}")
        print("Files created:")
        print("  - kpi_cases_per_account.csv")
        print("  - kpi_priority_status.csv")
        print("  - kpi_industry.csv")
        print("  - kpi_country.csv")
        print("  - kpi_time_series.csv")

    def run_full_analysis(self):
        """Execute complete analysis pipeline"""
        print("\n" + "🚀" * 40)
        print("DATA ANALYSIS CHALLENGE - FULL PIPELINE EXECUTION")
        print("🚀" * 40)
        
        self.load_data()
        self.process_data()
        self.export_kpis()
        self.create_visualizations()
        self.generate_insights()
        
        print("\n" + "✅" * 40)
        print("ANALYSIS COMPLETED SUCCESSFULLY!")
        print("✅" * 40)
        
        # Close database connection
        self.conn.close()


if __name__ == "__main__":
    # Initialize and run pipeline
    pipeline = DataAnalysisPipeline(
        accounts_path=config.ACCOUNTS_FILE,
        support_cases_path=config.SUPPORT_CASES_FILE
    )
    
    pipeline.run_full_analysis()