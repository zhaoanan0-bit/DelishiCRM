import streamlit as st
import pandas as pd
import sqlite3
import datetime
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import io
import os 
from datetime import timedelta

# --- 核心配置 ---
DB_FILE = 'crm_data.db' 

# --- 初始化与数据结构 (保持不变) ---
INITIAL_USERS = {
    'admin': {'password': 'admin123', 'role': 'admin', 'display_name': '超级管理员'},
    'zhaoxiaoan': {'password': 'zhaoxiaoan123', 'role': 'admin', 'display_name': '赵小安'},
    'liqiufang': {'password': '123', 'role': 'user', 'display_name': '李秋芳'}, 
    'fanqiuju': {'password': '123', 'role': 'user', 'display_name': '范秋菊'},
    'zhoumengke': {'password': '123', 'role': 'user', 'display_name': '周梦珂'},
}

SITE_OPTIONS = [
    "篮球馆（FIBA认证场地）", "排球馆", "羽毛球馆", "乒乓球馆", "室内网球场", "手球馆", "室内足球/五人制足球场",
    "学校体育馆", "幼儿园室内活动室", "小学/中学/大学多功能运动场", "室内操场/风雨操场",
    "综合健身房", "瑜伽馆、舞蹈室", "搏击/武术训练馆", "跨界训练（CrossFit）场地", "体能康复训练中心",
    "社区体育中心", "企事业单位职工活动中心", "商业连锁健身房", "青少年培训机构",
    "轮滑场", "壁球馆", "室内滑冰训练辅助区", "部队、公安、消防训练馆", "医院康复科运动治疗室", "老年活动中心", "其他/未分类"
]
SHOP_OPTIONS = ["天猫旗舰店", "拼多多运动店铺", "拼多多旗舰店", "淘宝店铺", "抖音店铺", "线下渠道/其他"]
STATUS_OPTIONS = ["初次接触", "已寄样", "报价中", "合同流程", "已签约", "施工中", "已完结/已收款", "流失/搁置", "已流失"]
INTENT_OPTIONS = ["高", "中", "低", "已成交", "流失", "已放弃"]
SOURCE_OPTIONS = ["自然进店", "拼多多推广", "天猫推广", "老客户转介绍", "其他"]
PROMO_TYPE_OPTIONS = ["成交收费", "成交加扣", "其他"]

# 映射字典
CRM_COL_MAP = {
    'id': 'ID', 'date': '录入日期', 'sales_rep': '对接人', 'customer_name': '客户名称',
    'phone': '联系电话', 'source': '客户来源', 'shop_name': '店铺名称', 'unit_price': '单价(元/㎡)',
    'area': '平方数(㎡)', 'site_type': '应用场地', 'status': '跟踪进度', 'is_construction': '是否施工',
    'construction_fee': '施工费(元)', 'material_fee': '辅料费(元)', 'shipping_fee': '运费(元)', 
    'purchase_intent': '购买意向', 'total_amount': '预估总金额(元)', 'follow_up_history': '跟进历史',
    'sample_no': '寄样单号', 'order_no': '订单号', 'last_follow_up_date': '上次跟进日期', 
    'next_follow_up_date': '计划下次跟进'
}
PROMO_COL_MAP = {
    'id': 'ID', 'month': '月份', 'shop': '店铺', 'promo_type': '推广类型',
    'total_spend': '总花费(元)', 'trans_spend': '成交花费(元)', 'net_gmv': '净成交额(元)',
    'net_roi': '净投产比(ROI)', 'cpa_net': '每笔净成交花费(元)', 'inquiry_count': '询单量',
    'inquiry_spend': '询单花费(元)', 'cpl': '询单成本(元/个)', 'note': '备注'
}

CN_TO_EN_MAP = {v: k for k, v in CRM_COL_MAP.items()}
DATABASE_COLUMNS = list(CRM_COL_MAP.keys())[1:] # 排除ID

COLUMN_REMAP = {
    '日期': '录入日期', '店铺名字': '店铺名称', '单价（元/㎡）': '单价(元/㎡)', '平方数（㎡）': '平方数(㎡)',
    '应用场地 ': '应用场地', '跟踪进度 ': '跟踪进度', '是否施工 ': '是否施工',
    '施工费（元）': '施工费(元)', '辅料费用（元）': '辅料费(元)', '购买意向 ': '购买意向',
    '总金额（元）': '预估总金额(元)', '备注': '跟进历史', '手机': '联系电话', '电话': '联系电话',
    '客户来源': '客户来源', '运费（元）': '运费(元)',
    '单价(元/m²)': '单价(元/㎡)', '平方数(m²)': '平方数(㎡)', '平方数（m²）': '平方数(㎡)', 
    '总金额(元)': '预估总金额(元)', '单价(元/平米)': '单价(元/㎡)', '平方数(平米)': '平方数(㎡)',
    '运费(元)': '运费(元)', '施工费(元)': '施工费(元)', '辅料费(元)': '辅料费(元)',
    '单价(元/平)': '单价(元/㎡)', '平方数(平)': '平方数(㎡)',
    '上次跟进日期': '上次跟进日期', '计划下次跟进': '计划下次跟进', # 确保导入时日期列名正确
}


# --- 数据库连接函数 ---
@st.cache_resource
def get_conn():
    return sqlite3.connect(DB_FILE, check_same_thread=False)

def init_db():
    conn = get_conn()
    c = conn.cursor()
    
    # 1. 用户表
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY, password TEXT, role TEXT, display_name TEXT
    )''')
    c.execute("SELECT COUNT(*) FROM users")
    if c.fetchone()[0] == 0:
        for u, d in INITIAL_USERS.items():
            c.execute("INSERT OR IGNORE INTO users VALUES (?, ?, ?, ?)", (u, d['password'], d['role'], d['display_name']))
    
    # 2. 销售表
    # 重新审视数据库列定义，确保TEXT字段足够宽，REAL字段用于数值
    c.execute('''CREATE TABLE IF NOT EXISTS sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT, sales_rep TEXT, customer_name TEXT, phone TEXT, source TEXT, shop_name TEXT,
        unit_price REAL, area REAL, site_type TEXT, status TEXT, is_construction TEXT,
        construction_fee REAL, material_fee REAL, shipping_fee REAL, purchase_intent TEXT,
        total_amount REAL, follow_up_history TEXT, sample_no TEXT, order_no TEXT,
        last_follow_up_date TEXT, next_follow_up_date TEXT
    )''')
    
    # 3. 推广表
    c.execute('''CREATE TABLE IF NOT EXISTS promotions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        month TEXT, shop TEXT, promo_type TEXT, total_spend REAL, trans_spend REAL,
        net_gmv REAL, net_roi REAL, cpa_net REAL, inquiry_count INTEGER,
        inquiry_spend REAL, cpl REAL, note TEXT
    )''')
    conn.commit()

# V11.2 增强：安全地将值转换为浮点数，对 None/空字符串/错误格式 返回 0.0
def get_safe_float(value):
    try:
        if value is None or (isinstance(value, str) and value.strip() == ''):
            return 0.0
        # 清理常见的非数字字符
        cleaned_value = str(value).replace(',', '').replace('¥', '').replace('$', '').strip()
        # 尝试转换
        return float(cleaned_value)
    except:
        return 0.0

# V11.2 新增：安全地将值转换为字符串，对 None 返回空字符串
def get_safe_string(value):
    return str(value) if value is not None else ''

# 检查并自动转交客户 (保持不变)
def check_and_transfer_customers(zhaoxiaoan_username):
    conn = get_conn()
    c = conn.cursor()
    today = datetime.date.today().isoformat()
    
    c.execute(f"""
        SELECT id, sales_rep, customer_name, last_follow_up_date 
        FROM sales 
        WHERE purchase_intent != '已成交' 
        AND (
            julianday('{today}') - julianday(last_follow_up_date) > 20 
            OR last_follow_up_date IS NULL
        )
        AND sales_rep != ? 
    """, (zhaoxiaoan_username,))
    
    records_to_transfer = c.fetchall()
    
    if records_to_transfer:
        transfer_count = 0
        for record_id, old_rep, name, last_date in records_to_transfer:
            log_entry = f"[{today} 系统自动]: 客户未成交且超过 20 天未跟进 ({last_date if last_date else '未曾跟进'})，自动转交给 {zhaoxiaoan_username} 管理。"
            
            # 执行转交
            c.execute("""
                UPDATE sales 
                SET sales_rep = ?, 
                    follow_up_history = follow_up_history || ?,
                    last_follow_up_date = ?
                WHERE id = ?
            """, (zhaoxiaoan_username, f"\n{log_entry}", today, record_id))
            transfer_count += 1
            
        conn.commit()
        if transfer_count > 0:
            st.warning(f"🚨 系统提醒：已自动将 {transfer_count} 个超期未跟进且未成交的客户转交给赵小安管理员处理。")
            return transfer_count
    return 0

# --- 核心 CRUD 函数 ---
def get_data(rename_cols=False):
    conn = get_conn()
    try:
        df = pd.read_sql_query("SELECT * FROM sales", conn)
        
        # 确保所有数值列转换为数字，防止 None/空字符串 导致计算或显示崩溃
        num_db_cols = ['unit_price', 'area', 'construction_fee', 'material_fee', 'shipping_fee', 'total_amount']
        for col in num_db_cols:
            if col in df.columns:
                # V11.2：使用安全转换，将所有非数值转换为 0.0
                df[col] = df[col].apply(get_safe_float)
                
        # 确保所有日期列可以安全转换为日期或 NaT
        date_cols = ['date', 'last_follow_up_date', 'next_follow_up_date']
        for col in date_cols:
            if col in df.columns:
                 # V11.2：将所有日期字段统一转换为日期对象，错误转为 NaT
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date.astype(str).replace({'NaT': None})

                
        if rename_cols: df.rename(columns=CRM_COL_MAP, inplace=True)
        return df
    except Exception as e: 
        st.error(f"数据库读取错误: {e}")
        return pd.DataFrame()

# V11.2 修复：在加载单个记录时，强制对数值和字符串进行安全转换
def get_single_record(record_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM sales WHERE id=?", (record_id,))
    record = c.fetchone()
    if record:
        cols = list(CRM_COL_MAP.keys())
        record_dict = dict(zip(cols, record))
        
        # 强制所有数值字段安全转换为 float/0.0
        num_cols = ['unit_price', 'area', 'construction_fee', 'material_fee', 'shipping_fee', 'total_amount']
        for col in num_cols:
            # 崩溃修复: 使用 get_safe_float 确保 number_input 接收的是 float
            record_dict[col] = get_safe_float(record_dict.get(col))
            
        # 强制所有字符串字段安全转换为 str/空字符串
        str_cols = ['customer_name', 'phone', 'source', 'shop_name', 'site_type', 'status', 'is_construction', 'purchase_intent', 'follow_up_history', 'sample_no', 'order_no']
        for col in str_cols:
             record_dict[col] = get_safe_string(record_dict.get(col))
             
        # 日期处理：安全地转换为日期对象或 None
        date_cols = ['date', 'last_follow_up_date', 'next_follow_up_date']
        for col in date_cols:
            date_str = get_safe_string(record_dict.get(col))
            try:
                record_dict[col] = pd.to_datetime(date_str, errors='coerce').date() if date_str else datetime.date.today()
            except:
                record_dict[col] = datetime.date.today()
            
        return record_dict
    return None

def add_data(data):
    conn = get_conn()
    c = conn.cursor()
    placeholders = ', '.join(['?'] * len(DATABASE_COLUMNS))
    c.execute(f"INSERT INTO sales ({', '.join(DATABASE_COLUMNS)}) VALUES ({placeholders})", data)
    conn.commit()

def update_data(record_id, data):
    conn = get_conn()
    c = conn.cursor()
    # 注意：follow_up_history, last_follow_up_date, date 不在常规更新范围内，应单独处理或排除
    update_cols = [col for col in DATABASE_COLUMNS if col not in ['follow_up_history', 'last_follow_up_date']]
    set_clause = ", ".join([f"{col}=?" for col in update_cols])
    
    # 强制将所有传入的数值字段转换为 float
    num_cols = ['unit_price', 'area', 'construction_fee', 'material_fee', 'shipping_fee', 'total_amount']
    for col in num_cols:
        data[col] = get_safe_float(data.get(col))
        
    # 强制将所有日期字段转换为 string (ISO format)
    date_cols = ['date', 'next_follow_up_date']
    for col in date_cols:
        data[col] = str(data.get(col))
        
    update_data_tuple = tuple(data[col] for col in update_cols) + (record_id,)
    
    sql = f"UPDATE sales SET {set_clause} WHERE id=?"
    c.execute(sql, update_data_tuple)
    conn.commit()


def update_follow_up(record_id, new_log, next_date, new_status, new_intent):
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT follow_up_history FROM sales WHERE id=?", (record_id,))
    old_log_result = c.fetchone()
    old_log = old_log_result[0] if old_log_result and old_log_result[0] else ""
    full_new_log = old_log + f"\n[{datetime.date.today()} {st.session_state['display_name']}]: {new_log}"
    
    # 强制日期为 ISO 格式的字符串
    today_str = datetime.date.today().isoformat()
    next_date_str = str(next_date)
    
    c.execute("""
        UPDATE sales 
        SET follow_up_history = ?, 
            last_follow_up_date = ?, next_follow_up_date = ?, status = ?, purchase_intent = ?
        WHERE id = ?
    """, (full_new_log.strip(), today_str, next_date_str, new_status, new_intent, record_id))
    conn.commit()

def delete_data(record_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM sales WHERE id=?", (record_id,))
    conn.commit()

# ... (用户信息/推广数据函数保持不变) ...

def get_user_info(username):
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT password, role, display_name FROM users WHERE username=?", (username,))
    res = c.fetchone()
    if res: return {'password': res[0], 'role': res[1], 'display_name': res[2]}
    return None

def get_user_map():
    conn = get_conn()
    df = pd.read_sql_query("SELECT username, display_name FROM users", conn)
    return df.set_index('username')['display_name'].to_dict()

def get_display_name_to_username_map():
    conn = get_conn()
    df = pd.read_sql_query("SELECT username, display_name FROM users", conn)
    return df.set_index('display_name')['username'].to_dict()

def get_promo_data(rename_cols=False):
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM promotions", conn)
    if rename_cols:
        valid_map = {k: v for k, v in PROMO_COL_MAP.items() if k in df.columns}
        df.rename(columns=valid_map, inplace=True)
    return df

def add_promo_data(data):
    conn = get_conn()
    c = conn.cursor()
    c.execute('''INSERT INTO promotions (
        month, shop, promo_type, total_spend, trans_spend, net_gmv, 
        net_roi, cpa_net, inquiry_count, inquiry_spend, cpl, note
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', data)
    conn.commit()


# 导入功能 (V11.2 修复：确保所有缺失字段安全填充，防止错位和乱码)
def import_data_from_excel(df_imported):
    conn = get_conn()
    c = conn.cursor()
    user_map_rev = get_display_name_to_username_map()
    
    # 清洗列名
    df_imported.columns = [col.strip() for col in df_imported.columns]
    df_imported.rename(columns=COLUMN_REMAP, inplace=True)
    
    if '客户名称' not in df_imported.columns:
        return False, "缺少必填列：客户名称"

    df_to_save = df_imported.copy()
    
    # V11.2: 补全缺失列并设定默认值/类型
    for cn_col, en_col in CN_TO_EN_MAP.items():
        if cn_col not in df_to_save.columns:
            if en_col in ['unit_price', 'area', 'construction_fee', 'material_fee', 'shipping_fee', 'total_amount']:
                df_to_save[cn_col] = 0.0 # 数值默认 0.0
            elif en_col in ['date', 'last_follow_up_date', 'next_follow_up_date']:
                 df_to_save[cn_col] = datetime.date.today().isoformat() # 日期默认今天
            else:
                df_to_save[cn_col] = '' # 字符串默认空
            
    # 将中文列名转换为数据库的英文列名
    df_to_save.rename(columns=CN_TO_EN_MAP, inplace=True)
    
    # 格式转换 (终极清洗)
    num_cols = ['unit_price', 'area', 'construction_fee', 'material_fee', 'shipping_fee'] 
    
    for col in num_cols:
        # V11.2: 强制使用 get_safe_float 清洗，解决导入的数字格式问题
        df_to_save[col] = df_to_save[col].apply(get_safe_float)
        
    # V11.2: 对接人映射和默认值
    df_to_save['sales_rep'] = df_to_save['sales_rep'].astype(str).apply(lambda x: user_map_rev.get(x.strip(), 'admin'))
    
    # V11.2: 日期字段处理 - 确保为 ISO 格式的字符串
    today = datetime.date.today().isoformat()
    date_cols = ['date', 'last_follow_up_date', 'next_follow_up_date']
    for col in date_cols:
        df_to_save[col] = pd.to_datetime(df_to_save[col], errors='coerce').dt.date.astype(str).replace({'NaT': today})

    # V11.2: 字符串字段清洗 - 确保没有 None
    str_cols = ['customer_name', 'phone', 'source', 'shop_name', 'site_type', 'status', 'is_construction', 'purchase_intent', 'follow_up_history', 'sample_no', 'order_no']
    for col in str_cols:
        df_to_save[col] = df_to_save[col].astype(str).replace({'None': ''}).fillna('')
        
    # 写入
    data_tuples = []
    for _, row in df_to_save.iterrows():
        # 导入时按公式重新计算 total_amount (不含运费)
        unit_price = row.get('unit_price', 0.0)
        area = row.get('area', 0.0)
        fee1 = row.get('construction_fee', 0.0)
        fee2 = row.get('material_fee', 0.0)
        
        calculated_total_amount = (unit_price * area) + fee1 + fee2 
        row['total_amount'] = calculated_total_amount

        # 确保所有列都有值，且顺序正确
        # 这里必须严格按照 DATABASE_COLUMNS 的顺序取值
        tup = tuple(row.get(c, '') for c in DATABASE_COLUMNS)
        data_tuples.append(tup)
        
    try:
        placeholders = ','.join(['?'] * len(DATABASE_COLUMNS))
        c.executemany(f"INSERT INTO sales ({','.join(DATABASE_COLUMNS)}) VALUES ({placeholders})", data_tuples)
        conn.commit()
        return True, len(df_imported)
    except Exception as e:
        return False, str(e)

# ... (模板创建函数保持不变) ...

def create_import_template():
    ordered_cols = [
        '录入日期', '对接人', '客户名称', '联系电话', '客户来源', '店铺名称', 
        '单价(元/㎡)', '平方数(㎡)', 
        '应用场地', '跟踪进度', '购买意向', '是否施工',
        '施工费(元)', '辅料费(元)', '运费(元)',
        '寄样单号', '订单号', '跟进历史', '上次跟进日期', '计划下次跟进'
    ]
    template_df = pd.DataFrame(columns=ordered_cols)
    example_data = {
        '录入日期': [datetime.date.today().isoformat(), (datetime.date.today() - datetime.timedelta(days=7)).isoformat()],
        '对接人': ['李秋芳', '周梦珂'], 
        '客户名称': ['张先生体育馆项目', '李女士学校采购'], 
        '联系电话': ['138xxxx8888', ''],
        '客户来源': ['自然进店', '老客户转介绍'],
        '店铺名称': ['天猫旗舰店', '拼多多运动店铺'],
        '单价(元/㎡)': [38.0, 42.5], 
        '平方数(㎡)': [500, 1200], 
        '应用场地': ['篮球馆（FIBA认证场地）', '学校体育馆'],
        '跟踪进度': ['已签约', '报价中'],
        '购买意向': ['已成交', '高'],
        '是否施工': ['是', '否'],
        '施工费(元)': [12500, 0],
        '辅料费(元)': [3000, 0],
        '运费(元)': [1500, 2000],
        '寄样单号': ['JS20250101', ''],
        '订单号': ['TM202501001', ''],
        '跟进历史': ['首次电话沟通，确定为大型项目', '已发报价单和样品图片'],
        '上次跟进日期': [datetime.date.today().isoformat(), (datetime.date.today() - datetime.timedelta(days=2)).isoformat()],
        '计划下次跟进': [(datetime.date.today() + datetime.timedelta(days=5)).isoformat(), (datetime.date.today() + datetime.timedelta(days=3)).isoformat()],
    }
    for col in ordered_cols:
        if col in example_data:
            template_df[col] = example_data[col]
        else:
            template_df[col] = ['' for _ in range(len(example_data['录入日期']))]

    return template_df

# --- 登录 (保持不变) ---
def check_password():
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False
    
    if not st.session_state["password_correct"]:
        st.header("🏢 CRM 系统登录")
        user = st.text_input("用户名")
        pwd = st.text_input("密码", type="password")
        if st.button("登录"):
            uinfo = get_user_info(user)
            if uinfo and uinfo['password'] == pwd:
                st.session_state["password_correct"] = True
                st.session_state["role"] = uinfo['role']
                st.session_state["user_now"] = user
                st.session_state["display_name"] = uinfo['display_name']
                st.rerun()
            else:
                st.error("密码错误")
        return False
    return True

# --- 提醒和转交逻辑 (V11.2 修复日期比较崩溃) ---
def display_reminders(df, current_user_username, user_map):
    today = datetime.date.today()
    
    # V11.2 修复：使用 errors='coerce' 将无效日期转为 NaT，并确保它们是日期对象以便比较
    # 注意：df['计划下次跟进'] 在 get_data 中已经是 ISO 格式的字符串或 None
    df['next_follow_up_date_dt'] = pd.to_datetime(df['计划下次跟进'], errors='coerce').dt.date
    
    # 1. 筛选当前用户的客户
    if st.session_state["role"] == 'user':
        # 映射回用户名进行筛选
        current_user_name = user_map.get(current_user_username, current_user_username)
        df_filtered = df[df['对接人'] == current_user_name].copy()
    else:
        # 管理员看所有人的提醒
        df_filtered = df.copy() 

    # 2. 找出超期客户 (今天 > 计划下次跟进日期)
    # 比较时必须使用 NaT.isna() 来处理无效日期，然后与 today 比较
    df_overdue = df_filtered[
        (df_filtered['next_follow_up_date_dt'].notna()) & 
        (df_filtered['next_follow_up_date_dt'] < today)
    ].sort_values('next_follow_up_date_dt')

    # 3. 找出未设置下次跟进的客户 (首次录入后未跟进)
    # next_follow_up_date_dt 为 None (即导入或录入时没有有效日期) 或 next_follow_up_date_dt 与 录入日期 相同
    df_no_fup = df_filtered[
        (df_filtered['next_follow_up_date_dt'].isna()) | 
        (df_filtered['计划下次跟进'].astype(str) == df_filtered['录入日期'].astype(str)) # 使用字符串比较，更安全
    ]

    # 4. 汇总提醒
    total_reminders = len(df_overdue) + len(df_no_fup)
    
    if total_reminders > 0:
        with st.expander(f"🔔 待处理跟进提醒 ({total_reminders} 个客户超期/待设置)", expanded=True):
            if not df_overdue.empty:
                st.error(f"🔴 **超期客户 (上次计划跟进日期已过，{len(df_overdue)} 个)**：")
                df_show = df_overdue[['ID', '客户名称', '对接人', '计划下次跟进', '跟踪进度', '购买意向']].copy()
                st.dataframe(df_show, hide_index=True)

            if not df_no_fup.empty:
                st.warning(f"🟡 **未设置下次跟进或首次录入客户 ({len(df_no_fup)} 个)**：")
                df_show = df_no_fup[['ID', '客户名称', '对接人', '录入日期', '跟踪进度', '购买意向']].copy()
                st.dataframe(df_show, hide_index=True)
    else:
        st.success("✅ 目前所有跟进计划都按时进行，暂无超期提醒。")
        

# --- 主程序 ---
def main():
    st.set_page_config(page_title="CRM全能版", layout="wide")
    init_db()

    if check_password():
        user_name = st.session_state["display_name"]
        role = st.session_state["role"]
        current_user_username = st.session_state["user_now"]
        user_map = get_user_map() # username -> display_name
        user_map_rev = get_display_name_to_username_map() # display_name -> username
        
        st.sidebar.title(f"👤 {user_name}")
        menu = ["📝 新增销售记录", "📊 数据追踪与查看", "📈 销售分析看板", "🌐 推广数据看板"]
        choice = st.sidebar.radio("菜单", menu)
        
        # 侧边栏：备份功能 
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 💾 数据备份")
        if st.sidebar.button("下载客户数据 (Excel)"):
            df_exp = get_data(rename_cols=True)
            if not df_exp.empty:
                df_exp['对接人'] = df_exp['对接人'].map(user_map).fillna(df_exp['对接人'])
                out = io.BytesIO()
                with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
                    df_exp.to_excel(writer, index=False)
                st.sidebar.download_button("📥 点击下载备份", data=out.getvalue(), file_name=f'CRM_Backup_{datetime.date.today()}.xlsx')
            else:
                st.sidebar.warning("暂无数据")

        # 1. 新增
        if choice == "📝 新增销售记录":
            st.subheader("📝 录入新客户")
            default_next_fup = datetime.date.today() + timedelta(days=3)
            with st.form("add_form", clear_on_submit=True):
                c1, c2, c3 = st.columns(3)
                date_val = c1.date_input("录入日期", datetime.date.today())
                name = c1.text_input("客户名称 (必填)")
                phone = c1.text_input("电话")
                source = c1.selectbox("来源", SOURCE_OPTIONS)
                
                shop = c2.selectbox("店铺", SHOP_OPTIONS)
                site = c2.selectbox("场地", SITE_OPTIONS)
                price = c2.number_input("单价(元/㎡)", 0.0, key='add_price') 
                area = c2.number_input("平方数(㎡)", 0.0, key='add_area') 
                
                is_const = c3.selectbox("是否施工", ["否", "是"])
                fee1 = c3.number_input("施工费(元)", 0.0, key='add_fee1') 
                fee2 = c3.number_input("辅料费(元)", 0.0, key='add_fee2') 
                fee3 = c3.number_input("运费(元) (独立计算)", 0.0, key='add_fee3') 
                
                total = (price * area) + fee1 + fee2 
                st.info(f"⚡️ 预估总金额 (不含运费，用于报表): ¥{total:,.2f}")

                st.markdown("---")
                c4, c5 = st.columns(2)
                intent = c4.selectbox("购买意向", INTENT_OPTIONS)
                status = c4.selectbox("跟踪进度", STATUS_OPTIONS)
                sample_no = c4.text_input("寄样单号")
                order_no = c4.text_input("订单号")
                
                next_fup = c5.date_input("计划下次跟进", default_next_fup)
                remark = c5.text_area("首次沟通记录")
                
                if st.form_submit_button("提交录入"):
                    if not name:
                        st.error("请输入客户名称")
                    else:
                        log_entry = f"[{datetime.date.today()} {user_name}]: 首次录入。{remark}"
                        
                        # V11.2: 强制所有字段为数据库要求的类型
                        data = (
                            str(date_val), current_user_username, name, phone, source, shop,
                            get_safe_float(price), get_safe_float(area), site, status, is_const, 
                            get_safe_float(fee1), get_safe_float(fee2), get_safe_float(fee3), intent, 
                            get_safe_float(total), log_entry, sample_no, order_no, 
                            str(date_val), str(next_fup) 
                        )
                        add_data(data)
                        st.success("录入成功！")
                        st.rerun()

        # 2. 列表
        elif choice == "📊 数据追踪与查看":
            st.subheader("📋 客户列表")
            df = get_data(rename_cols=True)
            
            # V11.2: 在数据加载后立即执行自动转交检查
            if 'transfer_check_done' not in st.session_state:
                zhaoxiaoan_username = 'zhaoxiaoan' 
                transferred_count = check_and_transfer_customers(zhaoxiaoan_username)
                st.session_state['transfer_check_done'] = True 
                if transferred_count > 0:
                    st.rerun() 

            
            if not df.empty:
                # 将内部的 sales_rep (username) 转换为 display_name
                df['对接人'] = df['对接人'].map(user_map).fillna(df['对接人'])
                # V11.2： display_reminders 修复了崩溃问题
                display_reminders(df, current_user_username, user_map) 

            # 快速跟进
            with st.expander("➕ 快速追加跟进"):
                if not df.empty:
                    
                    if role == 'user':
                        df_user_filtered = df[df['对接人'] == user_name].copy()
                        opts = [f"{r['ID']} - {r['客户名称']} ({r['对接人']})" for i, r in df_user_filtered.iterrows()]
                    else:
                        df_user_filtered = df 
                        opts = [f"{r['ID']} - {r['客户名称']} ({r['对接人']})" for i, r in df.iterrows()]
                        
                    sel = st.selectbox("选择客户", opts, key='fup_sel')
                    note = st.text_input("本次跟进情况")
                    
                    default_next_fup = datetime.date.today() + timedelta(days=3)
                    next_date = st.date_input("计划下次跟进", default_next_fup)
                    
                    up_status = st.selectbox("更新进度状态", STATUS_OPTIONS)
                    up_intent = st.selectbox("更新购买意向", INTENT_OPTIONS)

                    if st.button("提交跟进更新"):
                        if not sel: st.error("请先选择客户。")
                        else:
                            uid = int(sel.split(' - ')[0])
                            update_follow_up(uid, note, str(next_date), up_status, up_intent)
                            st.success("已更新")
                            st.session_state['transfer_check_done'] = False 
                            st.rerun()
                else: st.info("暂无客户数据可供跟进。")
            
            st.markdown("---")
            if not df.empty:
                # 过滤器
                c1, c2, c3 = st.columns(3)
                filter_user = c1.selectbox("筛选对接人", ["全部"] + list(user_map.values()))
                search = c3.text_input("搜索客户/电话")
                
                df_show = df.copy()
                
                if filter_user != "全部":
                    df_show = df_show[df_show['对接人'] == filter_user]
                if search:
                    df_show = df_show[df_show['客户名称'].astype(str).str.contains(search, case=False, na=False) | df_show['联系电话'].astype(str).str.contains(search, case=False, na=False)]
                
                # 列表显示顺序
                cols_to_show = [
                    'ID', '录入日期', '对接人', '客户名称', '联系电话', '店铺名称', 
                    '单价(元/㎡)', '平方数(㎡)', 
                    '预估总金额(元)', '运费(元)', 
                    '跟踪进度', '购买意向', '计划下次跟进', '跟进历史',
                    '是否施工', '施工费(元)', '辅料费(元)', '寄样单号', '订单号', '上次跟进日期' 
                ]
                
                # 格式化金额，确保显示
                df_show['预估总金额(元)'] = df_show['预估总金额(元)'].apply(lambda x: f"¥{get_safe_float(x):,.0f}")
                df_show['运费(元)'] = df_show['运费(元)'].apply(lambda x: f"¥{get_safe_float(x):,.0f}")
                df_show['施工费(元)'] = df_show['施工费(元)'].apply(lambda x: f"¥{get_safe_float(x):,.0f}")
                df_show['辅料费(元)'] = df_show['辅料费(元)'].apply(lambda x: f"¥{get_safe_float(x):,.0f}")

                # V11.2: 确保跟进历史等文本字段显示正常 (get_data已处理)
                
                st.dataframe(df_show[[c for c in cols_to_show if c in df_show.columns]], use_container_width=True, hide_index=True)
            
            # 管理员专属功能
            if role == 'admin':
                st.markdown("---")
                
                # 🛠️ 管理员编辑/删除客户 
                with st.expander("🛠️ 管理员编辑/删除客户"):
                    if not df.empty:
                        customer_ids = df['ID'].tolist()
                        edit_id = st.selectbox("选择要编辑或删除的客户ID", customer_ids, key='edit_id_sel')
                        # V11.2: get_single_record 强制安全类型转换
                        record = get_single_record(edit_id)
                        
                        if record:
                            st.markdown(f"#### 正在编辑客户 ID: {edit_id} ({record['customer_name']})")
                            
                            with st.form("edit_form"):
                                
                                current_rep_name = user_map.get(record['sales_rep'], record['sales_rep'])
                                
                                c1, c2, c3 = st.columns(3)
                                # 日期
                                # V11.2: 日期输入框接收 date 对象
                                new_date = c1.date_input("录入日期", record['date'])
                                new_name = c1.text_input("客户名称 (必填)", record['customer_name'])
                                new_phone = c1.text_input("联系电话", record['phone'])
                                
                                # 销售/店铺/场地
                                new_rep = c2.selectbox("对接人", options=list(user_map.values()), index=list(user_map.values()).index(current_rep_name) if current_rep_name in user_map.values() else 0)
                                new_shop = c2.selectbox("店铺名称", SHOP_OPTIONS, index=SHOP_OPTIONS.index(record['shop_name']) if record['shop_name'] in SHOP_OPTIONS else 0)
                                new_site = c2.selectbox("应用场地", SITE_OPTIONS, index=SITE_OPTIONS.index(record['site_type']) if record['site_type'] in SITE_OPTIONS else 0)
                                
                                # 金额和面积
                                # V11.2: 使用 record['area'] (已经是 float)
                                new_area = c3.number_input("平方数(㎡)", record['area'], min_value=0.0, key='edit_area')
                                new_price = c3.number_input("单价(元/㎡)", record['unit_price'], min_value=0.0, key='edit_price')
                                
                                # 费用
                                new_fee1 = st.number_input("施工费(元)", record['construction_fee'], min_value=0.0, key='edit_fee1')
                                new_fee2 = st.number_input("辅料费(元)", record['material_fee'], min_value=0.0, key='edit_fee2')
                                new_fee3 = st.number_input("运费(元) (独立计算)", record['shipping_fee'], min_value=0.0, key='edit_fee3')
                                
                                st.markdown("---")
                                
                                c4, c5 = st.columns(2)
                                
                                # 状态/意向
                                new_status = c4.selectbox("跟踪进度", STATUS_OPTIONS, index=STATUS_OPTIONS.index(record['status']) if record['status'] in STATUS_OPTIONS else 0)
                                new_intent = c4.selectbox("购买意向", INTENT_OPTIONS, index=INTENT_OPTIONS.index(record['purchase_intent']) if record['purchase_intent'] in INTENT_OPTIONS else 0)
                                new_is_const = c4.selectbox("是否施工", ["否", "是"], index=["否", "是"].index(record['is_construction']) if record['is_construction'] in ["否", "是"] else 0)
                                
                                # 单号
                                new_sample_no = c5.text_input("寄样单号", record['sample_no'])
                                new_order_no = c5.text_input("订单号", record['order_no'])
                                # V11.2: 日期输入框接收 date 对象
                                new_next_fup = c5.date_input("计划下次跟进", record['next_follow_up_date'])
                                
                                # 自动计算总金额 (不含运费)
                                new_total = (new_price * new_area) + new_fee1 + new_fee2
                                st.info(f"⚡️ 预估总金额(元) (自动计算，不含运费): ¥{new_total:,.2f}")
                                
                                c_sub, c_del = st.columns(2)
                                
                                if c_sub.form_submit_button("💾 确认修改"):
                                    if not new_name:
                                        st.error("客户名称不能为空")
                                    else:
                                        # V11.2: 强制将所有数据转换为安全类型
                                        updated_data = {
                                            'date': new_date,
                                            'sales_rep': user_map_rev.get(new_rep, new_rep),
                                            'customer_name': new_name,
                                            'phone': new_phone,
                                            'source': record['source'], 
                                            'shop_name': new_shop,
                                            'unit_price': new_price, 
                                            'area': new_area, 
                                            'site_type': new_site,
                                            'status': new_status,
                                            'is_construction': new_is_const,
                                            'construction_fee': new_fee1, 
                                            'material_fee': new_fee2, 
                                            'shipping_fee': new_fee3, 
                                            'purchase_intent': new_intent,
                                            'total_amount': new_total, 
                                            'sample_no': new_sample_no,
                                            'order_no': new_order_no,
                                            'next_follow_up_date': new_next_fup, # date object
                                        }
                                        update_data(edit_id, updated_data)
                                        st.success(f"客户ID {edit_id} 信息已更新！")
                                        st.session_state['transfer_check_done'] = False
                                        st.rerun()
                                
                                if c_del.button("🗑️ 警告: 删除客户", type="primary"):
                                    delete_data(edit_id)
                                    st.success(f"客户ID {edit_id} 已删除！")
                                    st.session_state['transfer_check_done'] = False
                                    st.rerun()

                    else: st.info("暂无数据可供编辑。")

                # ⬆️ 管理员导入 (Excel/CSV)
                with st.expander("⬆️ 管理员导入 (Excel/CSV)"):
                    st.warning("⚠️ 导入注意: 导入文件应严格按照核心必填列顺序，否则可能无法正确解析！请确保所有金额、面积、单价字段**不包含任何货币符号或千位分隔符**，否则可能导致错误。")
                    
                    # 模板下载
                    template_df = create_import_template()
                    out = io.BytesIO()
                    with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
                        template_df.to_excel(writer, index=False)
                    st.download_button("🔽 下载导入模板 (Excel)", data=out.getvalue(), file_name='CRM_Import_Template.xlsx', key='download_template')
                    
                    st.info(f"核心必填列: {', '.join(list(template_df.columns)[:5])}...")
                    up_file = st.file_uploader("上传文件", type=['xlsx', 'csv'], key='imp_file')
                    if up_file:
                        if st.button("确认导入", key='import_btn'):
                            try:
                                if up_file.name.endswith('.csv'): df_i = pd.read_csv(up_file)
                                else: df_i = pd.read_excel(up_file)
                                ok, msg = import_data_from_excel(df_i)
                                if ok: 
                                    st.success(f"导入成功 {msg} 条 (预估总金额已按公式重新计算)")
                                    st.session_state['transfer_check_done'] = False 
                                    st.rerun()
                                else: st.error(f"导入过程中发生致命错误: {msg}")
                            except Exception as e: st.error(f"文件读取错误: {e}")

        # 3. 销售分析 (保持不变)
        elif choice == "📈 销售分析看板":
            st.subheader("📈 核心销售数据分析 (仅统计 [已签约] 或 [已完结/已收款] 客户)")
            df = get_data(rename_cols=True)
            
            if df.empty:
                st.warning("暂无数据")
            else:
                st.sidebar.markdown("---")
                st.sidebar.markdown("### 🎯 目标设置")
                
                target_sales_default = 100000 
                target_sales = st.sidebar.number_input("💰 本月销售额目标 (元)", value=target_sales_default, min_value=1)
                
                target_area_default = 500
                target_area = st.sidebar.number_input("📐 本月销售面积目标 (㎡)", value=target_area_default, min_value=1)
                
                # --- 1. 数据清洗与筛选 ---
                ACQUIRED_STATUSES = ['已签约', '已完结/已收款']
                df_sold = df[df['跟踪进度'].isin(ACQUIRED_STATUSES)].copy()
                
                if df_sold.empty:
                    st.info("📊 本期尚未有客户达成 [已签约] 或 [已完结/已收款] 状态，无法进行成交分析。")
                    
                else:
                    # 实际成交数据
                    # V11.2：使用安全浮点数确保求和准确
                    total_sales = df_sold['预估总金额(元)'].apply(get_safe_float).sum()
                    total_area = df_sold['平方数(㎡)'].apply(get_safe_float).sum()
                    
                    # --- 2. KPI 展示 (基于成交数据和双目标) ---
                    st.markdown("#### ✅ 实际成交关键指标")
                    k1, k2, k3, k4 = st.columns(4)
                    
                    k1.metric("💰 实际总销售额", f"¥{total_sales:,.0f}")
                    k2.metric("📐 实际销售面积", f"{total_area:,.0f} ㎡")
                    
                    sales_completion_rate = min(total_sales / target_sales, 1.0) if target_sales > 0 else 0
                    k3.metric("📈 金额完成率", f"{sales_completion_rate*100:.1f}%", f"距目标差额: ¥{total_sales - target_sales:,.0f}")
                    
                    area_completion_rate = min(total_area / target_area, 1.0) if target_area > 0 else 0
                    k4.metric("📏 面积完成率", f"{area_completion_rate*100:.1f}%", f"距目标差额: {total_area - target_area:,.0f} ㎡")
                    
                    # --- 3. 图表 ---
                    st.markdown("---")
                    st.markdown("#### 📈 销售额分布与客户来源分析")
                    c1, c2 = st.columns(2)
                    
                    fig1 = px.pie(df_sold, names='店铺名称', values='预估总金额(元)', 
                                  title="实际成交额 - 店铺占比", hole=.3)
                    c1.plotly_chart(fig1, use_container_width=True)
                    
                    # V11.2：使用清洗后的数值进行图表绘制
                    df_source_sum = df_sold.copy()
                    df_source_sum['预估总金额(元)'] = df_source_sum['预估总金额(元)'].apply(get_safe_float)
                    df_source_sum = df_source_sum.groupby('客户来源')['预估总金额(元)'].sum().reset_index()
                    
                    fig2 = px.bar(df_source_sum, x='客户来源', y='预估总金额(元)', 
                                  title="实际成交额 - 客户来源", color='客户来源', 
                                  labels={'预估总金额(元)': '成交金额 (元)'})
                    c2.plotly_chart(fig2, use_container_width=True)

                    # --- 4. 龙虎榜 ---
                    st.markdown("---")
                    st.markdown("#### 🏆 销售龙虎榜")
                    
                    df['对接人'] = df['对接人'].map(user_map).fillna(df['对接人'])
                    df_sold['对接人'] = df_sold['对接人'].map(user_map).fillna(df_sold['对接人'])
                    
                    df_all_calc = df.copy()
                    df_all_calc['预估总金额(元)'] = df_all_calc['预估总金额(元)'].apply(get_safe_float)
                    
                    rank_total_amount = df_all_calc.groupby('对接人')['预估总金额(元)'].sum().reset_index()
                    rank_total_amount.rename(columns={'预估总金额(元)': '预估总金额(元) (所有客户)'}, inplace=True)
                    
                    df_sold_calc = df_sold.copy()
                    df_sold_calc['预估总金额(元)'] = df_sold_calc['预估总金额(元)'].apply(get_safe_float)
                    df_sold_calc['平方数(㎡)'] = df_sold_calc['平方数(㎡)'].apply(get_safe_float)
                    
                    rank_sold = df_sold_calc.groupby('对接人')[['预估总金额(元)', '平方数(㎡)']].sum().reset_index()
                    rank_sold.rename(columns={'预估总金额(元)': '成交总金额', '平方数(㎡)': '成交总面积'}, inplace=True)
                    
                    rank = pd.merge(rank_total_amount, rank_sold, on='对接人', how='outer').fillna(0)
                    rank = rank.sort_values('成交总金额', ascending=False)
                    
                    rank['成交总金额'] = rank['成交总金额'].apply(lambda x: f"¥{x:,.0f}")
                    rank['成交总面积'] = rank['成交总面积'].apply(lambda x: f"{x:,.0f} ㎡")
                    rank['预估总金额(元) (所有客户)'] = rank['预估总金额(元) (所有客户)'].apply(lambda x: f"¥{x:,.0f}")
                    
                    st.dataframe(rank.rename(columns={'对接人': '销售代表'}), use_container_width=True, hide_index=True)


        # 4. 推广看板 (保持不变)
        elif choice == "🌐 推广数据看板":
            st.subheader("🌐 推广数据")
            dfp = get_promo_data(rename_cols=True)
            
            with st.expander("➕ 录入推广数据"):
                col_m, col_s, col_t, col_c, col_g = st.columns(5)
                pm = col_m.date_input("月份", datetime.date.today(), format="YYYY/MM")
                ps = col_s.selectbox("店铺", SHOP_OPTIONS)
                pt = col_t.selectbox("类型", PROMO_TYPE_OPTIONS)
                cost = col_c.number_input("总花费", 0.0)
                gmv = col_g.number_input("净成交额", 0.0)
                
                if st.button("提交推广数据"):
                    cost_safe = get_safe_float(cost)
                    gmv_safe = get_safe_float(gmv)
                    roi = (gmv_safe / cost_safe) if cost_safe > 0 else 0
                    data_tuple = (str(pm)[:7], ps, pt, cost_safe, 0.0, gmv_safe, roi, 0.0, 0, 0.0, 0.0, "")
                    add_promo_data(data_tuple)
                    st.success("已录入")
                    st.rerun()
            
            if not dfp.empty:
                # V11.2：安全转换，确保计算正常
                dfp['总花费(元)'] = dfp['总花费(元)'].apply(get_safe_float)
                dfp['净成交额(元)'] = dfp['净成交额(元)'].apply(get_safe_float)
                dfp['净投产比(ROI)'] = dfp.apply(lambda row: row['净成交额(元)'] / row['总花费(元)'] if row['总花费(元)'] > 0 else 0, axis=1).round(2)

                st.dataframe(dfp, use_container_width=True, hide_index=True)
                
                dfp_group = dfp.groupby('月份')[['总花费(元)', '净成交额(元)']].sum().reset_index()
                fig = px.bar(dfp_group, x='月份', y=['总花费(元)', '净成交额(元)'], barmode='group', title="月度投入产出对比")
                st.plotly_chart(fig, use_container_width=True)

if __name__ == '__main__':
    main()