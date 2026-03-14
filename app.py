import streamlit as st
import pandas as pd
from datetime import datetime
import io
import re
import requests
import base64
from typing import Tuple, Optional, List, Dict

# ============================================================================
# CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="ET Coffee HFC Data Correction",
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={
        'About': "ET Coffee HFC Data Correction System v2.0"
    }
)

# Constants
GITHUB_OWNER = "mohammed-seid"
GITHUB_REPO = "hfc-data-private"
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "admin123"
ENUMERATOR_PASSWORD = "1234"
CACHE_TTL = 3600  # 1 hour

# ========== FILE NAMES ==========
CONSTRAINTS_FILE = "constraints_coffee.csv"
LOGIC_FILE = "logic_coffee.csv"
CORRECTIONS_FILE = "corrections_coffee.csv"

# ========== UPDATED ENUMERATOR LIST (Only 4 enumerators) ==========
VALID_ENUMERATORS = [
    "asfaw.m",
    "henok",
    "asfaw.f",
    "abrham.a"
]

# ============================================================================
# STYLING - Mobile-First Design
# ============================================================================

st.markdown("""
    <style>
    /* Mobile-optimized styles */
    .stButton>button {
        width: 100%;
        border-radius: 8px;
        height: 50px;
        font-size: 16px;
        font-weight: 600;
    }
    
    .stExpander {
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        margin-bottom: 12px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    
    .farmer-card {
        background: #f8f9fa;
        padding: 15px;
        border-radius: 8px;
        margin-bottom: 8px;
        border-left: 4px solid #4CAF50;
    }
    
    .farmer-info-row {
        display: flex;
        flex-wrap: wrap;
        gap: 15px;
        margin-top: 8px;
    }
    
    .farmer-info-item {
        display: flex;
        align-items: center;
        gap: 5px;
        font-size: 13px;
        color: #555;
    }
    
    .location-badge {
        background: #e3f2fd;
        color: #1565c0;
        padding: 3px 8px;
        border-radius: 4px;
        font-size: 12px;
        font-weight: 500;
    }
    
    .error-badge {
        background: #ff6b6b;
        color: white;
        padding: 4px 12px;
        border-radius: 12px;
        font-size: 12px;
        font-weight: 600;
    }
    
    .success-badge {
        background: #51cf66;
        color: white;
        padding: 4px 12px;
        border-radius: 12px;
        font-size: 12px;
        font-weight: 600;
    }
    
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 12px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    .enumerator-stats {
        background: #f8f9fa;
        padding: 16px;
        border-radius: 8px;
        margin-bottom: 12px;
        border-left: 4px solid #667eea;
    }
    
    .login-box {
        background: white;
        padding: 30px;
        border-radius: 12px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin: 20px 0;
    }
    
    /* Better mobile spacing */
    @media (max-width: 768px) {
        .stTextInput, .stNumberInput, .stTextArea {
            margin-bottom: 16px;
        }
        
        .stMetric {
            padding: 12px;
        }
        
        .farmer-info-row {
            flex-direction: column;
            gap: 8px;
        }
    }
    
    /* Progress indicator */
    .progress-bar {
        height: 8px;
        background: #e0e0e0;
        border-radius: 4px;
        overflow: hidden;
        margin: 16px 0;
    }
    
    .progress-fill {
        height: 100%;
        background: linear-gradient(90deg, #4CAF50, #8BC34A);
        transition: width 0.3s ease;
    }
    </style>
""", unsafe_allow_html=True)

# ============================================================================
# SESSION STATE INITIALIZATION
# ============================================================================

def initialize_session_state():
    """Initialize all session state variables"""
    defaults = {
        'corrected_errors': set(),
        'all_corrections_data': {},
        'is_admin': False,
        'is_authenticated': False,
        'selected_enumerator': None,
        'show_completed': False,
        'filter_error_type': 'All'
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

initialize_session_state()

# ============================================================================
# GITHUB API FUNCTIONS
# ============================================================================

def get_github_headers() -> Dict[str, str]:
    """Get GitHub API headers with authentication"""
    token = st.secrets.get("github", {}).get("token")
    if not token:
        raise ValueError("GitHub token not configured in secrets")
    
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }

def fetch_file_from_github(filename: str) -> Optional[pd.DataFrame]:
    """Fetch and parse CSV file from GitHub"""
    try:
        headers = get_github_headers()
        url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{filename}"
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            st.error(f"Failed to load {filename}: {response.status_code}")
            return None
        
        content = base64.b64decode(response.json()['content']).decode('utf-8')
        df = pd.read_csv(io.StringIO(content))
        
        return df
        
    except requests.exceptions.Timeout:
        st.error(f"⏱️ Timeout loading {filename}. Please check your connection.")
        return None
    except Exception as e:
        st.error(f"Error loading {filename}: {str(e)}")
        return None

@st.cache_data(ttl=CACHE_TTL)
def load_data_from_github() -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """Load constraints and logic data from GitHub with caching"""
    constraints_df = fetch_file_from_github(CONSTRAINTS_FILE)
    logic_df = fetch_file_from_github(LOGIC_FILE)
    
    if constraints_df is not None and logic_df is not None:
        st.success("✅ Data loaded from secure repository")
    
    return constraints_df, logic_df

def load_existing_corrections() -> Optional[pd.DataFrame]:
    """Load existing corrections from GitHub"""
    try:
        headers = get_github_headers()
        corrections_url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{CORRECTIONS_FILE}"
        response = requests.get(corrections_url, headers=headers)
        
        if response.status_code == 200:
            corrections_content = base64.b64decode(response.json()['content']).decode('utf-8')
            return pd.read_csv(io.StringIO(corrections_content))
        else:
            return None
    except:
        return None

def save_corrections_to_github(corrections_df: pd.DataFrame) -> bool:
    """Save or append corrections to GitHub"""
    try:
        headers = get_github_headers()
        corrections_url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{CORRECTIONS_FILE}"
        
        # Check if file exists and load existing data
        response = requests.get(corrections_url, headers=headers)
        sha = None
        
        if response.status_code == 200:
            sha = response.json()['sha']
            # Load existing corrections
            existing_content = base64.b64decode(response.json()['content']).decode('utf-8')
            existing_df = pd.read_csv(io.StringIO(existing_content))
            # Append new corrections
            corrections_df = pd.concat([existing_df, corrections_df], ignore_index=True)
        
        # Convert to CSV and encode
        csv_data = corrections_df.to_csv(index=False)
        encoded_data = base64.b64encode(csv_data.encode()).decode()
        
        # Prepare payload
        payload = {
            "message": f"Add coffee corrections - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "content": encoded_data,
            "branch": "main"
        }
        
        if sha:
            payload["sha"] = sha
            
        response = requests.put(corrections_url, headers=headers, json=payload, timeout=10)
        return response.status_code in [200, 201]
        
    except Exception as e:
        st.error(f"Error saving to GitHub: {str(e)}")
        return False

def check_token_validity() -> bool:
    """Verify GitHub token is valid"""
    try:
        headers = get_github_headers()
        response = requests.get("https://api.github.com/user", headers=headers, timeout=5)
        
        if response.status_code == 401:
            st.error("🔐 Access token expired. Please contact administrator.")
            return False
        return True
    except:
        return False

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_unique_id_column(df: pd.DataFrame) -> Optional[str]:
    """Find the unique ID column name in the dataframe"""
    if df is None or len(df) == 0:
        return None
    
    possible_names = [
        'unique_id', 'Unique_id', 'UNIQUE_ID', 'UniqueID', 'unique_ID',
        'id', 'ID', 'farmer_id', 'Farmer_ID', 'farmerid'
    ]
    
    for col_name in possible_names:
        if col_name in df.columns:
            return col_name
    
    for col in df.columns:
        if 'id' in col.lower():
            return col
    
    return None

def get_farmer_name_column(df: pd.DataFrame) -> Optional[str]:
    """Find the farmer name column in the dataframe"""
    if df is None or len(df) == 0:
        return None
    
    possible_names = [
        'farmer_name', 'resp_name', 'respondent_name', 'name', 
        'farmer', 'respondent', 'hh_name', 'hh_head_name'
    ]
    
    for col_name in possible_names:
        if col_name in df.columns:
            return col_name
    
    return None

def get_phone_column(df: pd.DataFrame) -> Optional[str]:
    """Find the phone number column in the dataframe"""
    if df is None or len(df) == 0:
        return None
    
    possible_names = [
        'phone_no', 'phone', 'telephone', 'mobile', 'contact',
        'phone_number', 'tel', 'cell'
    ]
    
    for col_name in possible_names:
        if col_name in df.columns:
            return col_name
    
    return None

def get_date_column(df: pd.DataFrame) -> Optional[str]:
    """Find the date column in the dataframe"""
    if df is None or len(df) == 0:
        return None
    
    possible_names = [
        'subdate', 'startdate', 'date', 'submission_date', 
        'interview_date', 'survey_date'
    ]
    
    for col_name in possible_names:
        if col_name in df.columns:
            return col_name
    
    return None

def get_reason_column(df: pd.DataFrame) -> Optional[str]:
    """Find the reason/constraint column in the dataframe"""
    if df is None or len(df) == 0:
        return None
    
    possible_names = [
        'reason', 'constraint', 'rule', 'validation', 
        'error_message', 'message', 'description'
    ]
    
    for col_name in possible_names:
        if col_name in df.columns:
            return col_name
    
    return None

def get_location_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    """Find location columns (woreda, kebele, village) in the dataframe"""
    location_cols = {
        'woreda': None,
        'kebele': None,
        'village': None
    }
    
    if df is None or len(df) == 0:
        return location_cols
    
    # Check for woreda
    woreda_names = ['woreda', 'Woreda', 'WOREDA', 'district', 'District']
    for name in woreda_names:
        if name in df.columns:
            location_cols['woreda'] = name
            break
    
    # Check for kebele
    kebele_names = ['kebele', 'Kebele', 'KEBELE', 'sub_district', 'village_admin']
    for name in kebele_names:
        if name in df.columns:
            location_cols['kebele'] = name
            break
    
    # Check for village
    village_names = ['village', 'Village', 'VILLAGE', 'gote', 'Gote', 'community']
    for name in village_names:
        if name in df.columns:
            location_cols['village'] = name
            break
    
    return location_cols

def safe_get_unique_ids(df: pd.DataFrame) -> set:
    """Safely get unique IDs from dataframe"""
    if df is None or len(df) == 0:
        return set()
    
    id_col = get_unique_id_column(df)
    if id_col is None:
        return set()
    
    return set(df[id_col].unique())

def format_display_value(value) -> str:
    """Format a value for display, handling None, NaN, and special values"""
    if value is None:
        return 'N/A'
    if pd.isna(value):
        return 'N/A'
    str_val = str(value).strip()
    if str_val in ['-99', '-999', 'nan', 'None', '']:
        return 'N/A'
    return str_val

# ============================================================================
# DATA PROCESSING FUNCTIONS
# ============================================================================

def extract_constraint_limits(constraint_text: str) -> Tuple[int, int]:
    """Extract min/max values from constraint text for display purposes only"""
    min_val, max_val = 0, 100000
    
    try:
        constraint_lower = str(constraint_text).lower()
        numbers = re.findall(r'\d+', constraint_text)
        
        if 'max' in constraint_lower and numbers:
            max_val = int(numbers[-1])
        if 'min' in constraint_lower and numbers:
            min_val = int(numbers[-1])
            
        if 'between' in constraint_lower and len(numbers) >= 2:
            min_val = int(numbers[0])
            max_val = int(numbers[1])
            
    except:
        pass
    
    return min_val, max_val

def get_corrected_error_keys(enumerator: str) -> set:
    """Get set of already corrected error keys for this enumerator"""
    existing_corrections = load_existing_corrections()
    
    if existing_corrections is None or len(existing_corrections) == 0:
        return set()
    
    enumerator_corrections = existing_corrections[
        existing_corrections['corrected_by'] == enumerator
    ]
    
    corrected_keys = set()
    for _, row in enumerator_corrections.iterrows():
        unique_id = None
        if 'unique_id' in row:
            unique_id = row['unique_id']
        else:
            for col in row.index:
                if 'id' in col.lower() and col != 'error_type':
                    unique_id = row[col]
                    break
        
        if unique_id:
            error_key = f"{row['error_type']}_{unique_id}_{row['variable']}"
            corrected_keys.add(error_key)
    
    return corrected_keys

def filter_uncorrected_errors(df: pd.DataFrame, error_type: str, enumerator: str) -> pd.DataFrame:
    """Remove already corrected errors from dataframe"""
    if df is None or len(df) == 0:
        return pd.DataFrame()
    
    id_col = get_unique_id_column(df)
    if id_col is None:
        return pd.DataFrame()
    
    corrected_keys = get_corrected_error_keys(enumerator)
    all_corrected = corrected_keys.union(st.session_state.corrected_errors)
    
    return df[~df.apply(
        lambda x: f"{error_type}_{x[id_col]}_{x['variable']}" in all_corrected,
        axis=1
    )]

def get_enumerator_statistics(constraints_df: pd.DataFrame, logic_df: pd.DataFrame) -> pd.DataFrame:
    """Get detailed statistics for each enumerator"""
    stats = []
    
    existing_corrections = load_existing_corrections()
    
    for enumerator in VALID_ENUMERATORS:
        constraint_errors = 0
        logic_errors = 0
        
        if constraints_df is not None and len(constraints_df) > 0:
            constraint_errors = len(constraints_df[constraints_df['username'] == enumerator])
        
        if logic_df is not None and len(logic_df) > 0:
            logic_errors = len(logic_df[logic_df['username'] == enumerator])
        
        total_errors = constraint_errors + logic_errors
        
        solved = 0
        if existing_corrections is not None:
            solved = len(existing_corrections[existing_corrections['corrected_by'] == enumerator])
        
        remaining = total_errors - solved
        percentage = (solved / total_errors * 100) if total_errors > 0 else 0
        
        stats.append({
            'Username': enumerator,
            'Total Errors': total_errors,
            'Solved': solved,
            'Remaining': remaining,
            'Progress (%)': round(percentage, 1)
        })
    
    stats_df = pd.DataFrame(stats)
    stats_df = stats_df.sort_values('Remaining', ascending=False)
    
    return stats_df

def get_comprehensive_error_analysis(constraints_df: pd.DataFrame, logic_df: pd.DataFrame) -> Dict:
    """Generate comprehensive error analysis summary"""
    analysis = {
        'error_type_overview': {},
        'error_rate_by_enumerator': [],
        'enumerators_without_errors': [],
        'most_common_variables': {},
        'strange_values': [],
        'overall_stats': {}
    }
    
    all_errors = []
    
    if constraints_df is not None and len(constraints_df) > 0:
        constraint_errors = constraints_df.copy()
        constraint_errors['error_category'] = 'Constraint'
        all_errors.append(constraint_errors)
    
    if logic_df is not None and len(logic_df) > 0:
        logic_errors = logic_df.copy()
        logic_errors['error_category'] = 'Logic'
        all_errors.append(logic_errors)
    
    if not all_errors:
        return analysis
    
    combined_errors = pd.concat(all_errors, ignore_index=True)
    
    id_col = get_unique_id_column(combined_errors)
    unique_farmers = combined_errors[id_col].nunique() if id_col else 0
    
    analysis['error_type_overview'] = {
        'Total Constraint Errors': len(constraints_df) if constraints_df is not None else 0,
        'Total Logic Errors': len(logic_df) if logic_df is not None else 0,
        'Total Errors': len(combined_errors),
        'Unique Farmers Affected': unique_farmers
    }
    
    enumerator_analysis = []
    for enumerator in VALID_ENUMERATORS:
        enum_errors = combined_errors[combined_errors['username'] == enumerator]
        constraint_count = len(enum_errors[enum_errors['error_category'] == 'Constraint'])
        logic_count = len(enum_errors[enum_errors['error_category'] == 'Logic'])
        total_count = len(enum_errors)
        
        if total_count > 0:
            existing_corrections = load_existing_corrections()
            solved = 0
            if existing_corrections is not None:
                solved = len(existing_corrections[existing_corrections['corrected_by'] == enumerator])
            
            error_rate = (total_count / analysis['error_type_overview']['Total Errors'] * 100) if analysis['error_type_overview']['Total Errors'] > 0 else 0
            
            enumerator_analysis.append({
                'Username': enumerator,
                'Constraint Errors': constraint_count,
                'Logic Errors': logic_count,
                'Total Errors': total_count,
                'Solved': solved,
                'Remaining': total_count - solved,
                'Error Rate (%)': round(error_rate, 2),
                'Completion Rate (%)': round((solved / total_count * 100), 2) if total_count > 0 else 0
            })
    
    analysis['error_rate_by_enumerator'] = pd.DataFrame(enumerator_analysis).sort_values('Total Errors', ascending=False)
    
    enumerators_with_errors = set(combined_errors['username'].unique())
    analysis['enumerators_without_errors'] = [e for e in VALID_ENUMERATORS if e not in enumerators_with_errors]
    
    variable_counts = combined_errors.groupby(['variable', 'error_category']).size().reset_index(name='count')
    variable_counts = variable_counts.sort_values('count', ascending=False)
    
    analysis['most_common_variables'] = {
        'top_constraint_variables': variable_counts[variable_counts['error_category'] == 'Constraint'].head(10),
        'top_logic_variables': variable_counts[variable_counts['error_category'] == 'Logic'].head(10),
        'overall_top_variables': variable_counts.head(15)
    }
    
    strange_values = []
    
    for _, row in combined_errors.iterrows():
        try:
            value = float(row['value'])
            error_cat = row['error_category']
            
            farmer_name_col = get_farmer_name_column(combined_errors)
            farmer_name = row.get(farmer_name_col, 'N/A') if farmer_name_col else 'N/A'
            
            reason_col = get_reason_column(combined_errors)
            reason = row.get(reason_col, row.get('constraint', 'N/A')) if reason_col else row.get('constraint', 'N/A')
            
            if value > 100000:
                strange_values.append({
                    'Type': f'{error_cat} - Extremely Large',
                    'Variable': row['variable'],
                    'Value': value,
                    'Username': row.get('username', 'N/A'),
                    'Farmer': farmer_name,
                    'Reason': reason
                })
            
            if value < 0 and 'temp' not in row['variable'].lower():
                strange_values.append({
                    'Type': f'{error_cat} - Negative Value',
                    'Variable': row['variable'],
                    'Value': value,
                    'Username': row.get('username', 'N/A'),
                    'Farmer': farmer_name,
                    'Reason': reason
                })
        except:
            pass
    
    analysis['strange_values'] = pd.DataFrame(strange_values) if strange_values else pd.DataFrame()
    
    enumerators_with_errors_count = len(enumerators_with_errors)
    analysis['overall_stats'] = {
        'Total Enumerators': len(VALID_ENUMERATORS),
        'Enumerators with Errors': enumerators_with_errors_count,
        'Enumerators without Errors': len(analysis['enumerators_without_errors']),
        'Average Errors per Enumerator': round(analysis['error_type_overview']['Total Errors'] / enumerators_with_errors_count, 2) if enumerators_with_errors_count > 0 else 0,
        'Unique Variables with Errors': combined_errors['variable'].nunique(),
        'Strange Values Detected': len(strange_values)
    }
    
    return analysis

# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

def validate_corrections() -> Tuple[bool, List[str], int, int]:
    """Validate all corrections are complete with explanations"""
    total_errors = len(st.session_state.all_corrections_data)
    completed = 0
    missing = []
    
    for error_key, correction_data in st.session_state.all_corrections_data.items():
        explanation = correction_data.get('explanation', '').strip()
        
        if not explanation:
            var_name = correction_data['error_data']['variable']
            error_type = "Constraint" if correction_data['error_type'] == 'constraint' else "Logic"
            missing.append(f"{error_type}: {var_name} - No explanation provided")
            continue
        
        if correction_data.get('outside_range', False):
            if len(explanation) < 20:
                var_name = correction_data['error_data']['variable']
                error_type = "Constraint" if correction_data['error_type'] == 'constraint' else "Logic"
                missing.append(f"{error_type}: {var_name} - Out-of-range value needs detailed explanation (min 20 chars)")
                continue
        
        completed += 1
    
    return completed == total_errors, missing, completed, total_errors

def validate_farmer_corrections(farmer_id: str) -> Tuple[bool, List[str], int, int]:
    """Validate corrections for a specific farmer"""
    farmer_corrections = {}
    
    for k, v in st.session_state.all_corrections_data.items():
        id_col = v.get('id_column', 'unique_id')
        farmer_id_val = v['error_data'].get(id_col)
        if str(farmer_id_val) == str(farmer_id):
            farmer_corrections[k] = v
    
    total_errors = len(farmer_corrections)
    completed = 0
    missing = []
    
    for error_key, correction_data in farmer_corrections.items():
        explanation = correction_data.get('explanation', '').strip()
        
        if not explanation:
            var_name = correction_data['error_data']['variable']
            error_type = "Constraint" if correction_data['error_type'] == 'constraint' else "Logic"
            missing.append(f"{error_type}: {var_name}")
            continue
        
        if correction_data.get('outside_range', False):
            if len(explanation) < 20:
                var_name = correction_data['error_data']['variable']
                missing.append(f"{var_name} - Needs detailed explanation")
                continue
        
        completed += 1
    
    return completed == total_errors, missing, completed, total_errors

# ============================================================================
# UI COMPONENTS
# ============================================================================

def render_progress_bar(current: int, total: int):
    """Render a visual progress bar"""
    percentage = (current / total * 100) if total > 0 else 0
    
    st.markdown(f"""
        <div class="progress-bar">
            <div class="progress-fill" style="width: {percentage}%"></div>
        </div>
        <p style="text-align: center; color: #666;">
            {current} of {total} completed ({percentage:.0f}%)
        </p>
    """, unsafe_allow_html=True)

def render_metric_card(label: str, value: str, icon: str = "📊"):
    """Render an attractive metric card"""
    st.markdown(f"""
        <div class="metric-card">
            <div style="font-size: 32px; margin-bottom: 8px;">{icon}</div>
            <div style="font-size: 28px; font-weight: 700;">{value}</div>
            <div style="font-size: 14px; opacity: 0.9;">{label}</div>
        </div>
    """, unsafe_allow_html=True)

def render_farmer_header(farmer_name: str, phone_no: str, woreda: str, kebele: str, village: str, error_count: int, completed_count: int = 0):
    """Render farmer information header with location details"""
    if completed_count > 0:
        badge = f'<span class="success-badge">{completed_count} ready</span> <span class="error-badge">{error_count - completed_count} pending</span>'
    else:
        badge = f'<span class="error-badge">{error_count} issues</span>'
    
    # Format display values
    phone_display = format_display_value(phone_no)
    woreda_display = format_display_value(woreda)
    kebele_display = format_display_value(kebele)
    village_display = format_display_value(village)
    
    st.markdown(f"""
        <div class="farmer-card">
            <div style="display: flex; justify-content: space-between; align-items: flex-start; flex-wrap: wrap;">
                <div style="flex: 1;">
                    <div style="font-size: 18px; font-weight: 600; margin-bottom: 8px;">👨‍🌾 {farmer_name}</div>
                    <div class="farmer-info-row">
                        <div class="farmer-info-item">
                            📞 <a href="tel:{phone_display}" style="color: #667eea; text-decoration: none;">{phone_display}</a>
                        </div>
                    </div>
                    <div class="farmer-info-row" style="margin-top: 10px;">
                        <div class="farmer-info-item">
                            <span class="location-badge">📍 Woreda: {woreda_display}</span>
                        </div>
                        <div class="farmer-info-item">
                            <span class="location-badge">🏘️ Kebele: {kebele_display}</span>
                        </div>
                        <div class="farmer-info-item">
                            <span class="location-badge">🏡 Village: {village_display}</span>
                        </div>
                    </div>
                </div>
                <div style="margin-top: 5px;">{badge}</div>
            </div>
        </div>
    """, unsafe_allow_html=True)

def render_constraint_error(error: pd.Series, error_key: str, id_col: str):
    """Render constraint error correction form"""
    st.markdown(f"### 🔒 {error['variable']}")
    
    reason_col = get_reason_column(pd.DataFrame([error]))
    constraint_text = error.get(reason_col, error.get('constraint', 'No constraint specified')) if reason_col else error.get('constraint', 'No constraint specified')
    
    min_val, max_val = extract_constraint_limits(str(constraint_text))
    
    try:
        default_value = int(float(error['value']))
    except:
        default_value = 0
    
    col1, col2 = st.columns([3, 2])
    
    with col1:
        st.info(f"**Current Value:** {error['value']}")
        st.caption(f"**Rule:** {constraint_text}")
        if min_val != 0 or max_val != 100000:
            st.caption(f"💡 Expected range: {min_val} - {max_val}")
    
    with col2:
        correct_value = st.number_input(
            "Corrected Value",
            value=default_value,
            step=1,
            key=f"value_{error_key}",
            help="Enter the actual correct value (no restrictions)"
        )
    
    outside_range = False
    if min_val != 0 or max_val != 100000:
        if correct_value < min_val or correct_value > max_val:
            st.warning(f"⚠️ Value is outside expected range ({min_val}-{max_val}). Please explain why in detail below.")
            outside_range = True
    
    explanation = st.text_area(
        "📝 Explanation (Required)",
        placeholder="Why is this correction needed? What did the farmer say? If outside expected range, provide detailed justification.",
        key=f"explain_{error_key}",
        height=120,
        help="Please provide a clear explanation for the correction"
    )
    
    st.session_state.all_corrections_data[error_key] = {
        'error_type': 'constraint',
        'error_data': error,
        'correct_value': correct_value,
        'explanation': explanation,
        'outside_range': outside_range,
        'id_column': id_col
    }
    
    if explanation and explanation.strip():
        if outside_range and len(explanation.strip()) < 20:
            st.warning("⚠️ Out-of-range value requires detailed explanation (at least 20 characters)")
        else:
            st.success("✅ Explanation provided")
    else:
        st.error("❌ Explanation required before saving")

def render_logic_error(error: pd.Series, error_key: str, id_col: str):
    """Render logic error correction form"""
    st.markdown(f"### 📊 {error['variable']}")
    
    reason_col = get_reason_column(pd.DataFrame([error]))
    reason_text = error.get(reason_col, error.get('reason', 'No reason specified')) if reason_col else error.get('reason', 'No reason specified')
    
    try:
        current_value = int(float(error['value']))
    except:
        current_value = 0
    
    min_val, max_val = extract_constraint_limits(str(reason_text))
    
    col1, col2 = st.columns([3, 2])
    
    with col1:
        st.info(f"**Current Value:** {error['value']}")
        st.caption(f"**Issue:** {reason_text}")
        if min_val != 0 or max_val != 100000:
            st.caption(f"💡 Expected range: {min_val} - {max_val}")
    
    with col2:
        correct_value = st.number_input(
            "Corrected Value",
            value=current_value,
            step=1,
            key=f"value_{error_key}",
            help="Enter the actual correct value after verification (no restrictions)"
        )
    
    outside_range = False
    if min_val != 0 or max_val != 100000:
        if correct_value < min_val or correct_value > max_val:
            st.warning(f"⚠️ Value is outside expected range ({min_val}-{max_val}). Please explain why in detail below.")
            outside_range = True
    
    explanation = st.text_area(
        "📝 Explanation (Required)",
        placeholder="Why is this the correct value? What did you verify with the farmer?",
        key=f"explain_{error_key}",
        height=120
    )
    
    st.session_state.all_corrections_data[error_key] = {
        'error_type': 'logic',
        'error_data': error,
        'correct_value': correct_value,
        'explanation': explanation,
        'outside_range': outside_range,
        'id_column': id_col
    }
    
    if explanation and explanation.strip():
        if outside_range and len(explanation.strip()) < 20:
            st.warning("⚠️ Out-of-range value requires detailed explanation (at least 20 characters)")
        else:
            st.success("✅ Explanation provided")
    else:
        st.error("❌ Explanation required before saving")

# ============================================================================
# SAVE FUNCTIONS
# ============================================================================

def save_farmer_corrections(farmer_id: str, selected_enumerator: str) -> bool:
    """Save corrections for a specific farmer"""
    farmer_corrections = {}
    for k, v in st.session_state.all_corrections_data.items():
        id_col = v.get('id_column', 'unique_id')
        if str(v['error_data'].get(id_col)) == str(farmer_id):
            farmer_corrections[k] = v
    
    if not farmer_corrections:
        return False
    
    corrections = []
    
    for error_key, correction_data in farmer_corrections.items():
        error_data = correction_data['error_data']
        id_col = correction_data.get('id_column', 'unique_id')
        
        farmer_name_col = get_farmer_name_column(pd.DataFrame([error_data]))
        phone_col = get_phone_column(pd.DataFrame([error_data]))
        date_col = get_date_column(pd.DataFrame([error_data]))
        reason_col = get_reason_column(pd.DataFrame([error_data]))
        
        base_record = {
            'error_type': correction_data['error_type'],
            'username': error_data.get('username', ''),
            'woreda': error_data.get('woreda', ''),
            'kebele': error_data.get('kebele', ''),
            'village': error_data.get('village', ''),
            'farmer_name': error_data.get(farmer_name_col, '') if farmer_name_col else error_data.get('resp_name', error_data.get('farmer_name', '')),
            'phone_no': error_data.get(phone_col, '') if phone_col else error_data.get('phone_no', ''),
            'subdate': error_data.get(date_col, '') if date_col else error_data.get('startdate', error_data.get('subdate', '')),
            'unique_id': error_data.get(id_col, ''),
            'variable': error_data.get('variable', ''),
            'original_value': error_data.get('value', ''),
            'correct_value': correction_data['correct_value'],
            'explanation': correction_data['explanation'],
            'corrected_by': selected_enumerator,
            'correction_date': datetime.now().strftime("%d-%b-%y"),
            'correction_timestamp': datetime.now().isoformat(),
            'outside_range': correction_data.get('outside_range', False)
        }
        
        if reason_col:
            base_record['reference_value'] = error_data.get(reason_col, '')
        else:
            base_record['reference_value'] = error_data.get('reason', error_data.get('constraint', ''))
        
        corrections.append(base_record)
    
    if corrections:
        corrections_df = pd.DataFrame(corrections)
        
        if save_corrections_to_github(corrections_df):
            for error_key in farmer_corrections.keys():
                st.session_state.corrected_errors.add(error_key)
                if error_key in st.session_state.all_corrections_data:
                    del st.session_state.all_corrections_data[error_key]
            return True
    
    return False

# ============================================================================
# AUTHENTICATION
# ============================================================================

def render_enumerator_login():
    """Render enumerator login page"""
    st.title("🔐 ET Coffee HFC Login")
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    # Enumerator Login
    with col1:
        st.markdown('<div class="login-box">', unsafe_allow_html=True)
        st.subheader("👤 Enumerator Login")
        
        with st.form("enumerator_login"):
            username = st.selectbox(
                "Select Username",
                options=[""] + VALID_ENUMERATORS,
                index=0
            )
            
            password = st.text_input("Password", type="password", key="enum_pass")
            
            submit = st.form_submit_button("🚀 Login", use_container_width=True, type="primary")
            
            if submit:
                if username and username in VALID_ENUMERATORS and password == ENUMERATOR_PASSWORD:
                    st.session_state.is_authenticated = True
                    st.session_state.selected_enumerator = username
                    st.session_state.is_admin = False
                    st.success(f"✅ Welcome, {username}!")
                    st.rerun()
                else:
                    st.error("❌ Invalid credentials")
        
        st.info("**Password:** `1234`")
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Admin Login
    with col2:
        st.markdown('<div class="login-box">', unsafe_allow_html=True)
        st.subheader("👑 Admin Login")
        
        with st.form("admin_login"):
            admin_user = st.text_input("Username", key="admin_user")
            admin_pass = st.text_input("Password", type="password", key="admin_pass")
            
            admin_submit = st.form_submit_button("🔑 Admin Login", use_container_width=True, type="secondary")
            
            if admin_submit:
                if admin_user == ADMIN_USERNAME and admin_pass == ADMIN_PASSWORD:
                    st.session_state.is_admin = True
                    st.session_state.is_authenticated = True
                    st.session_state.selected_enumerator = "admin"
                    st.success("✅ Admin access granted!")
                    st.rerun()
                else:
                    st.error("❌ Invalid admin credentials")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown("---")
    st.markdown(f"""
        ### 📋 Instructions
        
        **For Enumerators:**
        - Select your username from the dropdown
        - Available users: {', '.join(VALID_ENUMERATORS)}
        - Enter password: `1234`
        
        **For Administrators:**
        - Use admin credentials to access the dashboard
        - View progress across all enumerators
        - Download reports and statistics
    """)

# ============================================================================
# ADMIN DASHBOARD
# ============================================================================

def render_admin_dashboard(constraints_df: pd.DataFrame, logic_df: pd.DataFrame):
    """Render admin dashboard with enhanced analytics"""
    st.title("📊 ET Coffee HFC - Admin Dashboard")
    
    col1, col2 = st.columns([6, 1])
    with col2:
        if st.button("🚪 Logout", type="secondary", use_container_width=True):
            st.session_state.is_admin = False
            st.session_state.is_authenticated = False
            st.rerun()
    
    st.markdown("---")
    
    st.header("📈 High Frequency Check Summary")
    
    with st.spinner("Generating comprehensive analysis..."):
        analysis = get_comprehensive_error_analysis(constraints_df, logic_df)
    
    st.subheader("🎯 Error Type Overview")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        render_metric_card(
            "Total Errors", 
            str(analysis['error_type_overview']['Total Errors']), 
            "⚠️"
        )
    with col2:
        render_metric_card(
            "Constraint Errors", 
            str(analysis['error_type_overview']['Total Constraint Errors']), 
            "🔒"
        )
    with col3:
        render_metric_card(
            "Logic Errors", 
            str(analysis['error_type_overview']['Total Logic Errors']), 
            "📊"
        )
    with col4:
        render_metric_card(
            "Farmers Affected", 
            str(analysis['error_type_overview']['Unique Farmers Affected']), 
            "👨‍🌾"
        )
    
    st.markdown("---")
    
    st.subheader("👥 Error Rate by Enumerator")
    
    if not analysis['error_rate_by_enumerator'].empty:
        st.dataframe(
            analysis['error_rate_by_enumerator'],
            use_container_width=True,
            height=300
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Error Distribution**")
            fig_data = analysis['error_rate_by_enumerator']
            st.bar_chart(fig_data.set_index('Username')[['Constraint Errors', 'Logic Errors']])
        
        with col2:
            st.markdown("**Completion Rate**")
            st.bar_chart(analysis['error_rate_by_enumerator'].set_index('Username')['Completion Rate (%)'])
    
    st.markdown("---")
    
    st.subheader("✅ Enumerators Without Errors")
    
    if analysis['enumerators_without_errors']:
        st.success(f"**{len(analysis['enumerators_without_errors'])} enumerators** have no errors:")
        cols = st.columns(4)
        for idx, enum in enumerate(analysis['enumerators_without_errors']):
            with cols[idx % 4]:
                st.write(f"✅ {enum}")
    else:
        st.info("All enumerators have at least one error to correct")
    
    st.markdown("---")
    
    st.subheader("🔍 Most Frequent Variable Errors")
    
    tab1, tab2, tab3 = st.tabs(["📊 Overall", "🔒 Constraints", "📈 Logic"])
    
    with tab1:
        if not analysis['most_common_variables']['overall_top_variables'].empty:
            st.markdown("**Top 15 Variables with Most Errors**")
            top_vars = analysis['most_common_variables']['overall_top_variables']
            st.dataframe(
                top_vars.rename(columns={'count': 'Error Count', 'error_category': 'Type'}),
                use_container_width=True
            )
            st.bar_chart(top_vars.head(10).set_index('variable')['count'])
    
    with tab2:
        if not analysis['most_common_variables']['top_constraint_variables'].empty:
            st.markdown("**Top 10 Constraint Variables**")
            st.dataframe(
                analysis['most_common_variables']['top_constraint_variables'].rename(columns={'count': 'Error Count'}),
                use_container_width=True
            )
    
    with tab3:
        if not analysis['most_common_variables']['top_logic_variables'].empty:
            st.markdown("**Top 10 Logic Variables**")
            st.dataframe(
                analysis['most_common_variables']['top_logic_variables'].rename(columns={'count': 'Error Count'}),
                use_container_width=True
            )
    
    st.markdown("---")
    
    st.subheader("🚨 Strange & Outlier Values Detected")
    
    if not analysis['strange_values'].empty:
        st.warning(f"**{len(analysis['strange_values'])} suspicious values** detected that need attention:")
        
        strange_type_filter = st.multiselect(
            "Filter by type:",
            options=analysis['strange_values']['Type'].unique(),
            default=list(analysis['strange_values']['Type'].unique())
        )
        
        filtered_strange = analysis['strange_values'][
            analysis['strange_values']['Type'].isin(strange_type_filter)
        ]
        
        st.dataframe(
            filtered_strange,
            use_container_width=True,
            height=300
        )
        
        csv_strange = filtered_strange.to_csv(index=False)
        st.download_button(
            label="📥 Download Strange Values Report",
            data=csv_strange,
            file_name=f"strange_values_coffee_{datetime.now().strftime('%Y%m%d')}.csv",
            mime='text/csv'
        )
    else:
        st.success("✅ No suspicious outlier values detected")
    
    st.markdown("---")
    
    st.subheader("📊 Overall Statistics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Enumerators with Errors",
            analysis['overall_stats']['Enumerators with Errors'],
            delta=f"{analysis['overall_stats']['Enumerators without Errors']} clean"
        )
    
    with col2:
        st.metric(
            "Avg Errors/Enumerator",
            analysis['overall_stats']['Average Errors per Enumerator']
        )
    
    with col3:
        st.metric(
            "Unique Variables",
            analysis['overall_stats']['Unique Variables with Errors']
        )
    
    with col4:
        st.metric(
            "Strange Values",
            analysis['overall_stats']['Strange Values Detected'],
            delta="Need review" if analysis['overall_stats']['Strange Values Detected'] > 0 else "All good",
            delta_color="inverse"
        )
    
    st.markdown("---")
    
    stats_df = get_enumerator_statistics(constraints_df, logic_df)
    
    total_errors = stats_df['Total Errors'].sum()
    total_solved = stats_df['Solved'].sum()
    
    st.subheader("📈 Overall Progress")
    render_progress_bar(total_solved, total_errors)
    
    st.markdown("---")
    
    st.subheader("👥 Enumerator Statistics")
    
    filter_col1, filter_col2 = st.columns(2)
    
    with filter_col1:
        show_all = st.checkbox("Show all enumerators (including those with no errors)", value=False)
    
    with filter_col2:
        sort_by = st.selectbox(
            "Sort by",
            options=["Remaining (High to Low)", "Progress (%)", "Username", "Total Errors"],
            index=0
        )
    
    display_df = stats_df.copy()
    if not show_all:
        display_df = display_df[display_df['Total Errors'] > 0]
    
    if sort_by == "Remaining (High to Low)":
        display_df = display_df.sort_values('Remaining', ascending=False)
    elif sort_by == "Progress (%)":
        display_df = display_df.sort_values('Progress (%)', ascending=False)
    elif sort_by == "Username":
        display_df = display_df.sort_values('Username')
    elif sort_by == "Total Errors":
        display_df = display_df.sort_values('Total Errors', ascending=False)
    
    for idx, row in display_df.iterrows():
        with st.expander(f"👤 {row['Username']} - {row['Remaining']} remaining ({row['Progress (%)']}% complete)", expanded=False):
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Errors", row['Total Errors'])
            with col2:
                st.metric("Solved", row['Solved'], delta=row['Solved'])
            with col3:
                st.metric("Remaining", row['Remaining'], delta=-row['Remaining'], delta_color="inverse")
            with col4:
                st.metric("Progress", f"{row['Progress (%)']}%")
            
            render_progress_bar(row['Solved'], row['Total Errors'])
    
    st.markdown("---")
    
    st.subheader("📋 All Corrections")
    
    try:
        headers = get_github_headers()
        corrections_url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{CORRECTIONS_FILE}"
        response = requests.get(corrections_url, headers=headers)
        
        if response.status_code == 200:
            corrections_content = base64.b64decode(response.json()['content']).decode('utf-8')
            all_corrections = pd.read_csv(io.StringIO(corrections_content))
            
            filter_col1, filter_col2, filter_col3 = st.columns(3)
            
            with filter_col1:
                selected_enumerator = st.multiselect(
                    "Filter by Enumerator",
                    options=sorted(all_corrections['corrected_by'].unique()),
                    default=[]
                )
            
            with filter_col2:
                selected_error_type = st.multiselect(
                    "Filter by Error Type",
                    options=['constraint', 'logic'],
                    default=[]
                )
            
            with filter_col3:
                show_flagged = st.checkbox("Show flagged corrections only", value=False)
            
            filtered_df = all_corrections.copy()
            if selected_enumerator:
                filtered_df = filtered_df[filtered_df['corrected_by'].isin(selected_enumerator)]
            if selected_error_type:
                filtered_df = filtered_df[filtered_df['error_type'].isin(selected_error_type)]
            if 'outside_range' in filtered_df.columns and show_flagged:
                filtered_df = filtered_df[filtered_df['outside_range'] == True]
            
            st.markdown(f"**Showing {len(filtered_df)} of {len(all_corrections)} corrections**")
            
            st.dataframe(
                filtered_df.sort_values('correction_timestamp', ascending=False),
                use_container_width=True,
                height=400
            )
            
            if 'outside_range' in all_corrections.columns:
                out_of_range_count = all_corrections['outside_range'].sum()
                if out_of_range_count > 0:
                    st.warning(f"⚠️ {out_of_range_count} corrections have values outside expected range")
            
            st.markdown("---")
            st.subheader("💾 Download Data")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Filtered Data",
                    data=csv,
                    file_name=f"corrections_coffee_filtered_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    mime='text/csv',
                    use_container_width=True
                )
            
            with col2:
                csv_all = all_corrections.to_csv(index=False)
                st.download_button(
                    label="📥 Download All Corrections",
                    data=csv_all,
                    file_name=f"corrections_coffee_all_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    mime='text/csv',
                    use_container_width=True
                )
            
            with col3:
                stats_csv = stats_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Statistics",
                    data=stats_csv,
                    file_name=f"enumerator_stats_coffee_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    mime='text/csv',
                    use_container_width=True
                )
            
        else:
            st.info("📭 No corrections submitted yet.")
            
    except Exception as e:
        st.error(f"Error loading corrections data: {str(e)}")

# ============================================================================
# ENUMERATOR INTERFACE
# ============================================================================

def render_enumerator_interface(constraints_df: pd.DataFrame, logic_df: pd.DataFrame):
    """Render main enumerator correction interface"""
    
    selected_enumerator = st.session_state.selected_enumerator
    
    st.title("☕ ET Coffee HFC Data Correction")
    st.markdown(f"### Welcome, **{selected_enumerator}**")
    
    col1, col2 = st.columns([6, 1])
    with col2:
        if st.button("🚪 Logout", type="secondary", use_container_width=True):
            st.session_state.is_authenticated = False
            st.session_state.selected_enumerator = None
            st.rerun()
    
    st.markdown("---")
    
    constraint_id_col = get_unique_id_column(constraints_df)
    logic_id_col = get_unique_id_column(logic_df)
    
    if constraint_id_col is None and logic_id_col is None:
        st.error("❌ Cannot find unique ID column in data. Please check your CSV files.")
        st.info("Available columns in constraints: " + str(list(constraints_df.columns) if constraints_df is not None else "N/A"))
        st.info("Available columns in logic: " + str(list(logic_df.columns) if logic_df is not None else "N/A"))
        return
    
    id_col = constraint_id_col if constraint_id_col else logic_id_col
    
    enumerator_constraints = filter_uncorrected_errors(
        constraints_df[constraints_df['username'] == selected_enumerator] if constraints_df is not None else pd.DataFrame(),
        'constraint',
        selected_enumerator
    )
    
    enumerator_logic = filter_uncorrected_errors(
        logic_df[logic_df['username'] == selected_enumerator] if logic_df is not None else pd.DataFrame(),
        'logic',
        selected_enumerator
    )
    
    all_farmers_with_errors = sorted(
        safe_get_unique_ids(enumerator_constraints) | 
        safe_get_unique_ids(enumerator_logic)
    )
    
    if len(all_farmers_with_errors) == 0:
        st.success("🎉 All errors corrected! No pending issues.")
        st.balloons()
        return
    
    total_errors = len(enumerator_constraints) + len(enumerator_logic)
    
    existing_corrections = load_existing_corrections()
    saved_count = 0
    if existing_corrections is not None:
        saved_count = len(existing_corrections[existing_corrections['corrected_by'] == selected_enumerator])
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        render_metric_card("Farmers Pending", str(len(all_farmers_with_errors)), "👨‍🌾")
    with col2:
        render_metric_card("Issues Remaining", str(total_errors), "⚠️")
    with col3:
        render_metric_card("Already Saved", str(saved_count), "✅")
    
    st.markdown("---")
    
    error_filter = st.radio(
        "Filter by error type:",
        options=["All", "Constraints Only", "Logic Only"],
        horizontal=True
    )
    
    st.markdown("---")
    
    st.subheader("📞 Call Farmers & Correct Errors")
    st.caption("Complete corrections for each farmer and save individually, or save all at once")
    
    for farmer_id in all_farmers_with_errors:
        farmer_constraint_errors = enumerator_constraints[
            enumerator_constraints[id_col] == farmer_id
        ] if len(enumerator_constraints) > 0 else pd.DataFrame()
        
        farmer_logic_errors = enumerator_logic[
            enumerator_logic[id_col] == farmer_id
        ] if len(enumerator_logic) > 0 else pd.DataFrame()
        
        if error_filter == "Constraints Only" and len(farmer_constraint_errors) == 0:
            continue
        if error_filter == "Logic Only" and len(farmer_logic_errors) == 0:
            continue
        
        total_farmer_errors = len(farmer_constraint_errors) + len(farmer_logic_errors)
        
        if total_farmer_errors > 0:
            # Get farmer info dynamically
            farmer_name = ""
            phone_no = ""
            woreda = ""
            kebele = ""
            village = ""
            
            sample_df = farmer_constraint_errors if len(farmer_constraint_errors) > 0 else farmer_logic_errors
            if len(sample_df) > 0:
                farmer_name_col = get_farmer_name_column(sample_df)
                phone_col = get_phone_column(sample_df)
                location_cols = get_location_columns(sample_df)
                
                farmer_name = sample_df.iloc[0].get(farmer_name_col, 'Unknown') if farmer_name_col else sample_df.iloc[0].get('resp_name', sample_df.iloc[0].get('farmer_name', 'Unknown'))
                phone_no = sample_df.iloc[0].get(phone_col, 'N/A') if phone_col else sample_df.iloc[0].get('phone_no', 'N/A')
                
                # Get location info
                woreda = sample_df.iloc[0].get(location_cols['woreda'], '') if location_cols['woreda'] else sample_df.iloc[0].get('woreda', '')
                kebele = sample_df.iloc[0].get(location_cols['kebele'], '') if location_cols['kebele'] else sample_df.iloc[0].get('kebele', '')
                village = sample_df.iloc[0].get(location_cols['village'], '') if location_cols['village'] else sample_df.iloc[0].get('village', '')
            
            is_farmer_valid, farmer_missing, farmer_completed, farmer_total = validate_farmer_corrections(farmer_id)
            
            # Create expander title with location info
            phone_display = format_display_value(phone_no)
            woreda_display = format_display_value(woreda)
            
            with st.expander(f"👨‍🌾 {farmer_name} | 📍 {woreda_display} | 📞 {phone_display}", expanded=False):
                render_farmer_header(farmer_name, phone_no, woreda, kebele, village, total_farmer_errors, farmer_completed)
                
                st.markdown("---")
                
                if len(farmer_constraint_errors) > 0:
                    st.markdown("#### 🔒 Constraint Errors")
                    for idx, error in farmer_constraint_errors.iterrows():
                        error_key = f"constraint_{error[id_col]}_{error['variable']}"
                        render_constraint_error(error, error_key, id_col)
                        st.markdown("---")
                
                if len(farmer_logic_errors) > 0:
                    st.markdown("#### 📊 Logic Errors")
                    for idx, error in farmer_logic_errors.iterrows():
                        error_key = f"logic_{error[id_col]}_{error['variable']}"
                        render_logic_error(error, error_key, id_col)
                        st.markdown("---")
                
                st.markdown("---")
                
                if is_farmer_valid:
                    if st.button(f"💾 Save Corrections for {farmer_name}", key=f"save_{farmer_id}", type="primary", use_container_width=True):
                        with st.spinner("Saving..."):
                            if save_farmer_corrections(farmer_id, selected_enumerator):
                                st.success(f"✅ Saved {farmer_completed} corrections for {farmer_name}!")
                                st.balloons()
                                load_data_from_github.clear()
                                st.rerun()
                            else:
                                st.error("Failed to save. Please try again.")
                else:
                    st.warning(f"⚠️ Complete all corrections for this farmer to save ({farmer_completed}/{farmer_total} ready)")
                    with st.expander("Missing items"):
                        for item in farmer_missing:
                            st.write(f"• {item}")
    
    # Save all section
    st.markdown("---")
    st.header("💾 Save All Remaining Corrections")
    
    is_valid, missing_list, completed, total = validate_corrections()
    render_progress_bar(completed, total)
    
    if not is_valid:
        st.warning(f"⚠️ Some corrections are incomplete ({len(missing_list)} items)")
        with st.expander("See incomplete items"):
            for item in missing_list:
                st.write(f"• {item}")
    
    save_button_type = "primary" if is_valid else "secondary"
    
    if st.button("✅ Save All Completed Corrections", type=save_button_type, use_container_width=True, disabled=(completed == 0)):
        if completed == 0:
            st.error("No completed corrections to save")
            st.stop()
        
        corrections = []
        keys_to_remove = []
        
        for error_key, correction_data in st.session_state.all_corrections_data.items():
            explanation = correction_data.get('explanation', '').strip()
            
            if not explanation:
                continue
            
            if correction_data.get('outside_range', False) and len(explanation) < 20:
                continue
            
            error_data = correction_data['error_data']
            id_col = correction_data.get('id_column', 'unique_id')
            
            farmer_name_col = get_farmer_name_column(pd.DataFrame([error_data]))
            phone_col = get_phone_column(pd.DataFrame([error_data]))
            date_col = get_date_column(pd.DataFrame([error_data]))
            reason_col = get_reason_column(pd.DataFrame([error_data]))
            
            base_record = {
                'error_type': correction_data['error_type'],
                'username': error_data.get('username', ''),
                'woreda': error_data.get('woreda', ''),
                'kebele': error_data.get('kebele', ''),
                'village': error_data.get('village', ''),
                'farmer_name': error_data.get(farmer_name_col, '') if farmer_name_col else error_data.get('resp_name', error_data.get('farmer_name', '')),
                'phone_no': error_data.get(phone_col, '') if phone_col else error_data.get('phone_no', ''),
                'subdate': error_data.get(date_col, '') if date_col else error_data.get('startdate', error_data.get('subdate', '')),
                'unique_id': error_data.get(id_col, ''),
                'variable': error_data.get('variable', ''),
                'original_value': error_data.get('value', ''),
                'correct_value': correction_data['correct_value'],
                'explanation': correction_data['explanation'],
                'corrected_by': selected_enumerator,
                'correction_date': datetime.now().strftime("%d-%b-%y"),
                'correction_timestamp': datetime.now().isoformat(),
                'outside_range': correction_data.get('outside_range', False)
            }
            
            if reason_col:
                base_record['reference_value'] = error_data.get(reason_col, '')
            else:
                base_record['reference_value'] = error_data.get('reason', error_data.get('constraint', ''))
            
            corrections.append(base_record)
            keys_to_remove.append(error_key)
        
        if corrections:
            corrections_df = pd.DataFrame(corrections)
            
            with st.spinner("Saving to secure repository..."):
                if save_corrections_to_github(corrections_df):
                    st.success(f"✅ Successfully saved {len(corrections)} corrections!")
                    if total - completed > 0:
                        st.info(f"📝 {total - completed} items still need attention and were not saved.")
                    st.balloons()
                    
                    for error_key in keys_to_remove:
                        st.session_state.corrected_errors.add(error_key)
                        if error_key in st.session_state.all_corrections_data:
                            del st.session_state.all_corrections_data[error_key]
                    
                    load_data_from_github.clear()
                    st.rerun()
                else:
                    st.error("❌ Failed to save. Please try again or contact support.")
        else:
            st.warning("No completed corrections to save.")

# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    """Main application entry point"""
    
    if not st.session_state.is_authenticated:
        render_enumerator_login()
        return
    
    with st.spinner("Verifying access..."):
        if not check_token_validity():
            st.stop()
    
    with st.spinner("Loading data from secure repository..."):
        constraints_df, logic_df = load_data_from_github()
    
    if constraints_df is None or logic_df is None:
        st.error("❌ Could not load data from repository")
        st.info("""
            **Troubleshooting:**
            1. Check GitHub token is valid
            2. Verify files exist in repository
            3. Check internet connection
            4. Contact administrator if issue persists
        """)
        st.stop()
    
    if st.session_state.is_admin:
        render_admin_dashboard(constraints_df, logic_df)
    else:
        render_enumerator_interface(constraints_df, logic_df)
    
    st.markdown("---")
    st.markdown(
        "<p style='text-align: center; color: #666;'>☕ ET Coffee HFC Correction System v2.0 | "
        f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>",
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()