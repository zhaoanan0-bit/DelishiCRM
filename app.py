import streamlit as st
import pandas as pd
import sqlite3
import datetime
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import io
import os 

# --- 配置与数据初始化 ---
# 🚨 关键修复：使用内存数据库，确保在 Streamlit Cloud 上稳定运行
DB_FILE = ':memory:' 
PROMO_DB_FILE = ':memory:'
USER_DB_FILE = ':memory:'

DAYS_FOR_TRANSFER = 20 

# 1. 初始用户账号配置
INITIAL_USERS = {
    'admin': {'password': 'admin123', 'role': 'admin', 'display_name': '超级管理员'},
    'zhaoxiaoan': {'password': 'zhaoxiaoan123', 'role': 'admin', 'display_name': '赵小安'},
    'liqiufang': {'password': '123', 'role': 'user', 'display_name': '李秋芳'}, 
    'fanqiuju': {'password': '123', 'role': 'user', 'display_name': '范秋菊'},
    'zhoumengke': {'password': '123', 'role': 'user', 'display_name': '周梦珂'},
}

# 2. 下拉选项配置
SITE_OPTIONS = [
    "篮球馆（FIBA认证场地）", "排球馆", "羽毛球馆", "乒乓球馆", "室内网球场", "手球馆", "室内足球/五人制足球场",
    "学校体育馆", "幼儿园室内活动室", "小学/中学/大学多功能运动场", "室内操场/风雨操场",
    "综合健身房", "瑜伽馆、舞蹈室", "搏击/武术训练馆", "跨界训练（CrossFit）场地", "体能康复训练中心",
    "社区体育中心", "企事业单位职工活动中心", "商业连锁健身房", "青少年培训机构",
    "轮滑场", "壁球馆", "室内滑冰训练辅助区", "部队、公安、消防训练馆", "医院康复科运动治疗室", "老年活动中心", "其他/未分类"
]
SHOP_OPTIONS = ["天猫旗舰店", "拼多多运动店铺", "拼多多旗舰店", "淘宝店铺", "抖音店铺", "线下渠道/其他"]
STATUS_OPTIONS = ["初次接触", "已寄样", "报价中", "合同流程", "施工中", "已完结/已收款", "流失/搁置"]
INTENT_OPTIONS = ["高", "中", "低", "已成交", "流失"]
SOURCE_OPTIONS = ["自然进店", "拼多多推广", "天猫推广", "老客户转介绍", "其他"]
PROMO_TYPE_OPTIONS = ["成交收费", "成交加扣", "其他"]

# 3. 英文到中文列名映射 (核心部分)
CRM_COL_MAP = {
    'id': 'ID', 'date': '录入日期', 'sales_rep': '对接人', 'customer_name': '客户名称',
    'phone': '联系电话', 'source': '客户来源', 'shop_name': '店铺名称', 'unit_price': '单价(元/㎡)',
    'area': '平方数(㎡)', 'site_type': '应用场地', 'status': '跟踪进度', 'is_construction': '是否施工',
    'construction_fee': '施工费(元)', 'material_fee': '辅料费(元)', 'shipping_fee': '运费(元)', 
    'purchase_intent': '购买意向', 'total_amount': '预估总金额(元)', 'follow_up_history': '跟进历史',
    'sample_no': '寄样单号', 'order_no': '订单号', 'last_follow_up_date': '上次跟进日期', 
    'next_follow_up_date': '计划下次跟进'
}

# 推广数据列名映射
PROMO_COL_MAP = {
    'id': 'ID', 'month': '月份', 'shop': '店铺', 'promo_type': '推广类型',
    'total_spend': '总花费(元)', 'trans_spend': '成交花费(元)', 'net_gmv': '净成交额(元)',
    'net_roi': '净投产比(ROI)', 'cpa_net': '每笔净成交花费(元)', 'inquiry_count': '询单量',
    'inquiry_spend': '询单花费(元)', 'cpl': '询单成本(元/个)', 'note': '备注'
}

# 中文到英文列名映射（用于导入时转换）
CN_TO_EN_MAP = {v: k for k, v in CRM_COL_MAP.items()}
# 导入时必须包含的列（不含 ID，不含自动计算项）
REQUIRED_IMPORT_COLUMNS = [
    '录入日期', '对接人', '客户名称', '联系电话', '客户来源', '店铺名称', '单价(元/㎡)', '平方数(㎡)', 
    '应用场地', '跟踪进度', '是否施工', '施工费(元)', '辅料费(元)', '运费(元)', '购买意向', 
    '跟进历史', '寄样单号', '订单号', '上次跟进日期', '计划下次跟进'
]

# --- 数据库连接函数（全部使用内存模式）---
def get_user_conn():
    # 🚨 关键修复：每次连接都确保表结构和初始数据存在
    conn = sqlite3.connect(USER_DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY,
        password TEXT,
        role TEXT,
        display_name TEXT
    )''')
    
    # 只有当用户表为空时，才插入初始用户
    c.execute("SELECT COUNT(*) FROM users")
    if c.fetchone()[0] == 0:
        for username, data in INITIAL_USERS.items():
            # 使用 OR IGNORE 确保多次调用不会重复插入
            c.execute("INSERT OR IGNORE INTO users VALUES (?, ?, ?, ?)", 
                      (username, data['password'], data['role'], data['display_name']))
    conn.commit()
    return conn # 返回连接对象


def get_crm_conn():
    return sqlite3.connect(DB_FILE)

def get_promo_conn():
    return sqlite3.connect(PROMO_DB_FILE)


# --- 数据库函数 (用户管理) ---
# 🚨 移除了 init_user_db()，其功能已被 get_user_conn() 吸收。

def get_all_users():
    conn = get_user_conn()
    df = pd.read_sql_query("SELECT username, role, display_name FROM users", conn)
    conn.close()
    return df

def get_user_info(username):
    conn = get_user_conn()
    c = conn.cursor()
    c.execute("SELECT password, role, display_name FROM users WHERE username=?", (username,))
    result = c.fetchone()
    conn.close()
    if result:
        return {'password': result[0], 'role': result[1], 'display_name': result[2]}
    return None

def add_new_user(username, password, role, display_name):
    conn = get_user_conn()
    c = conn.cursor()
    try:
        c.execute("INSERT INTO users VALUES (?, ?, ?, ?)", (username, password, role, display_name))
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        conn.close()
        return False

def get_user_map():
    df = get_all_users()
    return df.set_index('username')['display_name'].to_dict()

def get_display_name_to_username_map():
    df = get_all_users()
    return df.set_index('display_name')['username'].to_dict()

# --- 数据库函数 (CRM 客户数据) ---
def init_db():
    conn = get_crm_conn()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        sales_rep TEXT,
        customer_name TEXT,
        phone TEXT,              
        source TEXT,             
        shop_name TEXT,
        unit_price REAL,
        area REAL,
        site_type TEXT,
        status TEXT,
        is_construction TEXT,
        construction_fee REAL,
        material_fee REAL,
        shipping_fee REAL,
        purchase_intent TEXT,
        total_amount REAL,
        follow_up_history TEXT,  
        sample_no TEXT,
        order_no TEXT,
        last_follow_up_date TEXT, 
        next_follow_up_date TEXT   
    )''')
    conn.commit()
    conn.close()

def get_data(rename_cols=False):
    conn = get_crm_conn()
    df = pd.read_sql_query("SELECT * FROM sales", conn)
    conn.close()
    
    if rename_cols:
        df.rename(columns=CRM_COL_MAP, inplace=True)
    
    return df

def add_data(data):
    conn = get_crm_conn()
    c = conn.cursor()
    c.execute('''INSERT INTO sales (
        date, sales_rep, customer_name, phone, source, shop_name, unit_price, area, 
        site_type, status, is_construction, construction_fee, material_fee, shipping_fee,
        purchase_intent, total_amount, follow_up_history, sample_no, order_no,
        last_follow_up_date, next_follow_up_date 
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', data)
    conn.commit()
    conn.close()

# 🚨 新增：批量导入功能
def import_data_from_excel(df_imported):
    conn = get_crm_conn()
    c = conn.cursor()
    display_to_user_map = get_display_name_to_username_map()
    
    # 1. 检查并重命名列
    df_imported.columns = [col.strip() for col in df_imported.columns] # 去除空格
    
    missing_cols = [col for col in REQUIRED_IMPORT_COLUMNS if col not in df_imported.columns]
    if missing_cols:
        return False, f"缺少以下必填列，请检查表格抬头：{', '.join(missing_cols)}"

    # 仅选择需要导入的列，并将其转换为数据库的英文列名
    df_to_save = df_imported[REQUIRED_IMPORT_COLUMNS].copy()
    df_to_save.rename(columns=CN_TO_EN_MAP, inplace=True)
    
    # 2. 数据清洗和计算
    df_to_save['date'] = pd.to_datetime(df_to_save['date'], errors='coerce').dt.strftime('%Y-%m-%d')
    df_to_save['last_follow_up_date'] = pd.to_datetime(df_to_save['last_follow_up_date'], errors='coerce').dt.strftime('%Y-%m-%d')
    df_to_save['next_follow_up_date'] = pd.to_datetime(df_to_save['next_follow_up_date'], errors='coerce').dt.strftime('%Y-%m-%d')

    # 确保数值列是数字，空值填 0
    num_cols = ['unit_price', 'area', 'construction_fee', 'material_fee', 'shipping_fee']
    for col in num_cols:
        df_to_save[col] = pd.to_numeric(df_to_save[col], errors='coerce').fillna(0.0)

    # 计算 total_amount (预估总金额，不含运费)
    df_to_save['total_amount'] = (df_to_save['unit_price'] * df_to_save['area']) + df_to_save['construction_fee'] + df_to_save['material_fee']
    
    # 3. 对接人映射：将中文名转换为 username
    df_to_save['sales_rep'] = df_to_save['sales_rep'].apply(lambda x: display_to_user_map.get(x, 'admin'))

    # 4. 插入数据库
    columns = [
        'date', 'sales_rep', 'customer_name', 'phone', 'source', 'shop_name', 'unit_price', 'area', 
        'site_type', 'status', 'is_construction', 'construction_fee', 'material_fee', 'shipping_fee',
        'purchase_intent', 'total_amount', 'follow_up_history', 'sample_no', 'order_no',
        'last_follow_up_date', 'next_follow_up_date' 
    ]
    
    data_tuples = []
    for index, row in df_to_save.iterrows():
        # 严格按照数据库列顺序构建元组
        data_tuples.append((
            row['date'], row['sales_rep'], row['customer_name'], row['phone'], row['source'], row['shop_name'],
            row['unit_price'], row['area'], row['site_type'], row['status'], row['is_construction'],
            row['construction_fee'], row['material_fee'], row['shipping_fee'], row['purchase_intent'],
            row['total_amount'], row['follow_up_history'], row['sample_no'], row['order_no'],
            row['last_follow_up_date'], row['next_follow_up_date']
        ))

    try:
        c.executemany(f'''INSERT INTO sales ({', '.join(columns)}) 
                          VALUES ({', '.join(['?']*len(columns))})''', data_tuples)
        conn.commit()
        conn.close()
        return True, len(df_imported)
    except Exception as e:
        conn.close()
        return False, f"数据库写入失败：{e}"


# ... (其他 CRUD 函数保持不变) ...
def get_single_record(record_id):
    conn = get_crm_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM sales WHERE id=?", (record_id,))
    columns = [desc[0] for desc in c.description]
    record = c.fetchone()
    conn.close()
    if record:
        return dict(zip(columns, record))
    return None

def admin_update_data(record_id, data):
    conn = get_crm_conn()
    c = conn.cursor()
    # 🚨 更改逻辑：总金额不再包含运费
    total_amount = (data['unit_price'] * data['area']) + data['construction_fee'] + data['material_fee'] 
    
    c.execute('''UPDATE sales SET
        customer_name=?, phone=?, source=?, shop_name=?, unit_price=?, area=?, 
        site_type=?, is_construction=?, construction_fee=?, material_fee=?, shipping_fee=?,
        total_amount=?
        WHERE id=?''', (
        data['customer_name'], data['phone'], data['source'], data['shop_name'], data['unit_price'], data['area'], 
        data['site_type'], data['is_construction'], data['construction_fee'], data['material_fee'], data['shipping_fee'],
        total_amount, record_id
    ))
    conn.commit()
    conn.close()
    update_follow_up(record_id, "[管理员修改]: 基本信息(不含运费)已更新，金额已重算。", 
                     datetime.date.today().isoformat(), data['status'], data['purchase_intent'])

def delete_data(record_id):
    conn = get_crm_conn()
    c = conn.cursor()
    c.execute("DELETE FROM sales WHERE id=?", (record_id,))
    conn.commit()
    conn.close()

def transfer_sales_rep(record_id, new_rep_username):
    conn = get_crm_conn()
    c = conn.cursor()
    user_info = get_user_info(new_rep_username)
    display_name = user_info['display_name'] if user_info else new_rep_username
    log = f"\n[{datetime.date.today()}] 系统转交：已转交给 {display_name}"
    c.execute("UPDATE sales SET sales_rep=?, status='转交管理', last_follow_up_date=?, follow_up_history=follow_up_history || ? WHERE id=?", 
              (new_rep_username, datetime.date.today().isoformat(), log, record_id))
    conn.commit()
    conn.close()

def update_follow_up(record_id, new_log, next_date, new_status, new_intent):
    conn = get_crm_conn()
    c = conn.cursor()
    c.execute("""
        UPDATE sales 
        SET follow_up_history = follow_up_history || ?, 
            last_follow_up_date = ?, 
            next_follow_up_date = ?,
            status = ?,
            purchase_intent = ?
        WHERE id = ?
    """, (f"\n{new_log}", datetime.date.today().isoformat(), next_date, new_status, new_intent, record_id))
    conn.commit()
    conn.close()

def check_customer_exist(name, phone):
    conn = get_crm_conn()
    c = conn.cursor()
    # 增加对 phone 字段非空判断，避免空电话号码导致误判
    c.execute("SELECT sales_rep FROM sales WHERE customer_name=? OR (phone IS NOT NULL AND phone != '' AND phone=?)", (name, phone))
    result = c.fetchone()
    conn.close()
    return result[0] if result else None

# --- 管理员功能：批量修复单价/面积互换 ---
def admin_fix_area_price_swap():
    conn = get_crm_conn()
    c = conn.cursor()
    
    # 1. 临时交换 unit_price 和 area
    c.execute("UPDATE sales SET unit_price = area, area = unit_price")
    
    # 2. 重新计算 total_amount (🚨 更改逻辑：不包含运费)
    c.execute("""
        UPDATE sales 
        SET total_amount = (unit_price * area) + construction_fee + material_fee
    """)
    
    # 3. 记录操作
    log_message = f"\n[{datetime.date.today()}] [系统管理员操作]: 批量修复单价和面积数据互换，并重新计算了**不含运费**的总金额。"
    c.execute("UPDATE sales SET follow_up_history = follow_up_history || ?", (log_message,))
    
    conn.commit()
    rows_affected = c.rowcount
    conn.close()
    return rows_affected

# --- 数据库函数 (推广数据) ---
def init_promo_db():
    conn = get_promo_conn()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS promotions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        month TEXT,
        shop TEXT,
        promo_type TEXT,
        total_spend REAL,
        trans_spend REAL,
        net_gmv REAL,
        net_roi REAL,
        cpa_net REAL,
        inquiry_count INTEGER,
        inquiry_spend REAL,
        cpl REAL,
        note TEXT
    )''')
    conn.commit()
    conn.close()

def add_promo_data(data):
    conn = get_promo_conn()
    c = conn.cursor()
    c.execute('''INSERT INTO promotions (
        month, shop, promo_type, total_spend, trans_spend, net_gmv, 
        net_roi, cpa_net, inquiry_count, inquiry_spend, cpl, note
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', data)
    conn.commit()
    conn.close()

def get_promo_data(rename_cols=False):
    conn = get_promo_conn()
    df = pd.read_sql_query("SELECT * FROM promotions", conn)
    conn.close()
        
    if rename_cols:
        # 🚨 确保只重命名存在的列
        valid_rename_map = {k: v for k, v in PROMO_COL_MAP.items() if k in df.columns}
        df.rename(columns=valid_rename_map, inplace=True)
    return df

# --- 登录逻辑 ---
def check_password():
    def password_entered():
        user_info = get_user_info(st.session_state["username"])
        if user_info and st.session_state["password"] == user_info['password']:
            st.session_state["password_correct"] = True
            st.session_state["role"] = user_info['role']
            st.session_state["user_now"] = st.session_state["username"]
            st.session_state["display_name"] = user_info['display_name']
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.header("🏢 CRM 系统登录")
        st.text_input("用户名", key="username")
        st.text_input("密码", type="password", key="password")
        st.button("登录", on_click=password_entered)
        return False
    elif not st.session_state["password_correct"]:
        st.header("🏢 CRM 系统登录")
        st.text_input("用户名", key="username")
        st.text_input("密码", type="password", key="password")
        st.button("登录", on_click=password_entered)
        st.error("用户不存在或密码错误")
        return False
    else:
        return True

# --- 主程序 ---
def main():
    st.set_page_config(page_title="CRM运营全能版", layout="wide")
    # 🚨 关键修复：移除了 init_user_db() 的调用。
    init_db()
    init_promo_db()

    if check_password():
        user_role = st.session_state["role"]
        current_user = st.session_state["user_now"]
        current_display_name = st.session_state["display_name"]
        user_map = get_user_map()
        
        st.sidebar.title(f"👤 {current_display_name}")
        menu = ["📝 新增销售记录", "📊 数据追踪与查看", "📈 销售分析看板", "🌐 推广数据看板"]
        choice = st.sidebar.radio("菜单", menu)
        
        # --- 侧边栏：数据导出 ---
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 💾 数据备份")
        
        if st.sidebar.button("下载客户数据 (Excel)"):
            # 获取原始英文列名数据
            df_export = get_data(rename_cols=False) 
            if not df_export.empty:
                # 复制并重命名列，用于导出
                df_export_cn = df_export.rename(columns=CRM_COL_MAP)
                
                # 导出时将对接人从 username 映射为中文名
                df_export_cn['对接人'] = df_export_cn['对接人'].map(user_map).fillna(df_export_cn['对接人'])
                
                output = io.BytesIO()
                # 计算一个"实际含运费总额"字段供参考
                df_export_cn['实际含运费总额(元)'] = df_export_cn['预估总金额(元)'] + df_export_cn['运费(元)']
                
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    # 仅导出中文列名的子集，避免导出 days_since_fup 等临时列
                    cols_to_export = list(CRM_COL_MAP.values()) + ['实际含运费总额(元)']
                    df_export_cn[[c for c in cols_to_export if c in df_export_cn.columns]].to_excel(writer, index=False, sheet_name='Sheet1')
                    
                excel_data = output.getvalue()
                st.sidebar.download_button(label="📥 客户数据备份", data=excel_data, file_name=f'CRM_Customer_Backup_{datetime.date.today()}.xlsx', mime='application/vnd.ms-excel')
            else:
                st.sidebar.warning("暂无客户数据")
        
        if st.sidebar.button("下载推广数据 (Excel)"):
            df_promo_export = get_promo_data(rename_cols=True) # 导出时使用中文列名
            if not df_promo_export.empty:
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_promo_export.to_excel(writer, index=False, sheet_name='Sheet1')
                excel_data = output.getvalue()
                st.sidebar.download_button(label="📥 推广数据备份", data=excel_data, file_name=f'CRM_Promo_Backup_{datetime.date.today()}.xlsx', mime='application/vnd.ms-excel')
            else:
                st.sidebar.warning("暂无推广数据")


        # 1. 新增记录页面
        if choice == "📝 新增销售记录":
             st.subheader("📝 客户信息录入")
             with st.form("entry_form", clear_on_submit=True):
                 col1, col2, col3 = st.columns(3)
                 with col1:
                     date_val = st.date_input("录入日期", datetime.date.today())
                     customer_name = st.text_input("客户名称 (必填)")
                     phone = st.text_input("联系电话 (用于查重)")
                     source = st.selectbox("客户来源", SOURCE_OPTIONS)
                 
                 with col2:
                     shop_name = st.selectbox("店铺名字", SHOP_OPTIONS)
                     site_type = st.selectbox("应用场地", SITE_OPTIONS)
                     unit_price = st.number_input("单价 (元/㎡)", min_value=0.0, step=0.1)
                     area = st.number_input("平方数 (㎡)", min_value=0.0, step=0.1)
                 
                 with col3:
                     is_const = st.selectbox("是否施工", ["否", "是"])
                     const_fee = st.number_input("施工费 (元)", min_value=0.0, step=100.0)
                     mat_fee = st.number_input("辅料费用 (元)", min_value=0.0, step=50.0)
                     shipping_fee = st.number_input("运费 (元)", min_value=0.0, step=10.0)
                     st.text_input("对接人", value=current_display_name, disabled=True)

                 st.markdown("---")
                 col4, col5 = st.columns(2)
                 with col4:
                     purchase_intent = st.selectbox("购买意向", INTENT_OPTIONS)
                     status = st.selectbox("跟踪进度", STATUS_OPTIONS)
                     sample_no = st.text_input("寄样单号")
                     order_no = st.text_input("订单号")
                 with col5:
                     last_fup = st.date_input("🗓️ 首次跟进日期", datetime.date.today())
                     next_fup = st.date_input("🚨 计划下次跟进", datetime.date.today() + datetime.timedelta(days=3))
                     first_remark = st.text_area("首次沟通记录")
                 
                 # 🚨 预估总金额不含运费
                 preview_total = (unit_price * area) + const_fee + mat_fee
                 st.caption(f"💰 **预估总金额** (不含运费): **{preview_total:,.2f}** 元")
                 st.caption(f"🚚 运费: {shipping_fee:,.2f} 元 | 实际总价(含运): **{(preview_total + shipping_fee):,.2f}** 元")


                 submitted = st.form_submit_button("✅ 提交录入")

                 if submitted:
                     if customer_name == "":
                         st.warning("⚠️ 客户名称不能为空")
                     else:
                         existing_rep = check_customer_exist(customer_name, phone)
                         if existing_rep:
                             rep_display_name = user_map.get(existing_rep, existing_rep)
                             st.error(f"❌ 录入失败！该客户已存在，目前由 **{rep_display_name}** 负责。")
                         else:
                             # 🚨 calc_total 不含运费
                             calc_total = (unit_price * area) + const_fee + mat_fee
                             log_entry = f"[{datetime.date.today()} {current_display_name}]: 首次录入。{first_remark}"
                             
                             data_tuple = (
                                 date_val, current_user, customer_name, phone, source, shop_name, unit_price, area,
                                 site_type, status, is_const, const_fee, mat_fee, shipping_fee,
                                 purchase_intent, calc_total, log_entry, sample_no, order_no,
                                 str(last_fup), str(next_fup)
                             )
                             add_data(data_tuple)
                             st.success(f"🎉 客户 {customer_name} 录入成功！")


        # 2. 数据查看页面 
        elif choice == "📊 数据追踪与查看":
             st.subheader("📋 客户追踪列表")
             
             # 获取原始英文列名数据，然后立即转换为中文列名
             df = get_data(rename_cols=True) 
             
             with st.expander("➕ 快速追加跟进记录"):
                 col_up1, col_up2 = st.columns([1, 2])
                 with col_up1:
                     if not df.empty:
                         # 这里的 df 已经使用了中文列名 '对接人'，但内容是 username
                         df['中文对接人'] = df['对接人'].map(user_map).fillna(df['对接人'])
                         customer_id_map = {f"{row['ID']} - {row['客户名称']} ({row['中文对接人']})": row['ID'] for index, row in df.iterrows()}
                         selected_customer_label = st.selectbox("选择客户 ID 和名称", list(customer_id_map.keys()) if customer_id_map else [])
                         up_id = customer_id_map.get(selected_customer_label, None)
                     else:
                         up_id = st.number_input("输入客户 ID", min_value=1, step=1)
                 with col_up2:
                     up_content = st.text_input("本次跟进情况")
                 
                 col_up3, col_up4, col_up5 = st.columns(3)
                 with col_up3:
                     up_next_date = st.date_input("下次跟进时间", datetime.date.today() + datetime.timedelta(days=3))
                 with col_up4:
                     up_status = st.selectbox("更新进度状态", STATUS_OPTIONS, key="up_stat")
                 with col_up5:
                     up_intent = st.selectbox("更新购买意向", INTENT_OPTIONS, key="up_int")
                 
                 if st.button("🚀 提交跟进更新"):
                     if up_id is None:
                          st.error("请先录入数据。")
                     elif not df.empty and up_id in df['ID'].values: 
                        # 获取原始 username (在 df 中仍是 username)
                        record_rep_username = df[df['ID'] == up_id]['对接人'].values[0] 
                        
                        if user_role == 'admin' or record_rep_username == current_user:
                            new_log = f"[{datetime.date.today()} {current_display_name}]: {up_content}"
                            update_follow_up(up_id, new_log, str(up_next_date), up_status, up_intent)
                            st.success("跟进记录已追加！")
                            st.rerun()
                        else:
                            st.error("无权限操作非本人客户记录。")
                     else:
                         st.error("ID 不存在")

             st.markdown("---")
             
             if not df.empty:
                 # df 此时已经是中文列名
                 df_show = df.copy()
                 
                 # 日期处理
                 df_show['计划下次跟进'] = pd.to_datetime(df_show['计划下次跟进'], errors='coerce')
                 df_show['上次跟进日期'] = pd.to_datetime(df_show['上次跟进日期'], errors='coerce')
                 df_show['录入日期'] = pd.to_datetime(df_show['录入日期'], errors='coerce') 
                 today = datetime.date.today()
                 
                 df_show['days_since_fup'] = (pd.to_datetime(today) - df_show['上次跟进日期']).dt.days
                 
                 # 映射 '对接人' 列内容为中文名 (如果需要筛选显示)
                 df_show['中文对接人'] = df_show['对接人'].map(user_map).fillna(df_show['对接人'])
                 
                 # 超期转交逻辑
                 overdue = df_show[(df_show['跟踪进度'] != '已完结/已收款') & (df_show['days_since_fup'] > DAYS_FOR_TRANSFER)]
                 if user_role == 'admin' and not overdue.empty:
                     st.error(f"⚠️ 管理员注意：有 {len(overdue)} 个客户超 {DAYS_FOR_TRANSFER} 天未跟进！")
                     if st.button("🔥 一键接管所有超期客户"):
                         # 必须使用原始ID进行转交
                         overdue_ids = overdue['ID'].values
                         for pid in overdue_ids:
                             transfer_sales_rep(pid, 'admin')
                         st.success("已全部转入管理员名下")
                         st.rerun()

                 # 提醒逻辑
                 my_reminders = df_show[
                     (df_show['计划下次跟进'].dt.date <= today) & 
                     (df_show['跟踪进度'] != '已完结/已收款') &
                     (df_show['中文对接人'] == current_display_name)
                 ]
                 if not my_reminders.empty:
                     st.warning(f"🔔 {current_display_name}，您今天有 {len(my_reminders)} 个待办跟进！")

                 col_filter_month, col_filter_rep, col_search = st.columns(3)
                 
                 with col_filter_month:
                     df_show['录入年月'] = df_show['录入日期'].dt.strftime('%Y年%m月')
                     month_options = ['全部月份'] + sorted(df_show['录入年月'].unique().tolist(), reverse=True)
                     filter_month = st.selectbox("🗓️ 录入月份筛选", month_options)
                     
                 with col_filter_rep:
                     rep_display_options = ['全部'] + list(user_map.values())
                     filter_rep_display = st.selectbox("👤 对接人筛选", rep_display_options)

                 with col_search:
                     search_term = st.text_input("🔍 搜客户、电话或店铺")

                 df_final = df_show.copy()
                 
                 if filter_month != '全部月份':
                     df_final = df_final[df_final['录入年月'] == filter_month]
                     
                 if filter_rep_display != '全部':
                     # 筛选基于中文名
                     df_final = df_final[df_final['中文对接人'] == filter_rep_display]
                 
                 if search_term:
                     df_final = df_final[
                         df_final['客户名称'].astype(str).str.contains(search_term, case=False) |
                         df_final['联系电话'].astype(str).str.contains(search_term, case=False) |
                         df_final['店铺名称'].astype(str).str.contains(search_term, case=False)
                     ]
                 
                 # 最终显示：使用中文对接人，但保留原始对接人字段（username）用于后台操作。
                 # 将中文对接人覆盖到 '对接人' 列，用于显示。
                 df_final['对接人'] = df_final['中文对接人']
                 
                 # 定义 Streamlit 列配置
                 st_col_config = {
                    "ID": st.column_config.NumberColumn("ID"),
                    "录入日期": st.column_config.DateColumn("录入日期"),
                    "对接人": st.column_config.TextColumn("👤 对接人"),
                    "客户名称": st.column_config.TextColumn("客户名称"),
                    "联系电话": st.column_config.TextColumn("联系电话"),
                    "客户来源": st.column_config.TextColumn("客户来源"),
                    "店铺名称": st.column_config.TextColumn("店铺名称"),
                    "单价(元/㎡)": st.column_config.NumberColumn("单价(元/㎡)", format="%.2f"),
                    "平方数(㎡)": st.column_config.NumberColumn("平方数(㎡)", format="%.2f"),
                    "应用场地": st.column_config.TextColumn("应用场地"),
                    "跟踪进度": st.column_config.TextColumn("跟踪进度"),
                    "是否施工": st.column_config.TextColumn("是否施工"),
                    "施工费(元)": st.column_config.NumberColumn("施工费(元)", format="%.2f"),
                    "辅料费(元)": st.column_config.NumberColumn("辅料费(元)", format="%.2f"),
                    "运费(元)": st.column_config.NumberColumn("运费(元)", format="%.2f"), # 运费单独列
                    "购买意向": st.column_config.TextColumn("购买意向"),
                    "预估总金额(元)": st.column_config.NumberColumn("预估总金额(元)", format="¥%.2f", help="不含运费的总金额"), 
                    "寄样单号": st.column_config.TextColumn("寄样单号"),
                    "订单号": st.column_config.TextColumn("订单号"),
                    "跟进历史": st.column_config.TextColumn("📜 跟进历史", width="large"),
                    "上次跟进日期": st.column_config.DateColumn("上次跟进"),
                    "计划下次跟进": st.column_config.DateColumn("计划下次"),
                    "days_since_fup": None, # 隐藏计算列
                    "录入年月": None, # 隐藏计算列
                    "中文对接人": None # 隐藏计算列
                 }
                 
                 # 确保只选择 CRM_COL_MAP 中定义的中文列名 
                 display_cols = list(CRM_COL_MAP.values()) 
                 df_display = df_final[[c for c in display_cols if c in df_final.columns]].copy()

                 # 🚨 最终显示
                 st.dataframe(
                     df_display,
                     hide_index=True, 
                     use_container_width=True,
                     column_config=st_col_config
                 )

                 # --- 管理员功能区 ---
                 if user_role == 'admin':
                     st.markdown("---")
                     st.subheader("🛠️ 管理员操作区")
                     
                     # 导入功能集成在这里
                     with st.expander("📥 批量导入客户数据 (Excel/CSV)"):
                         st.warning("导入前请注意：导入文件需**完全匹配**以下所有列名，否则导入会失败！")
                         st.markdown(f"**必填列名:** `{', '.join(REQUIRED_IMPORT_COLUMNS)}`")
                         
                         uploaded_file = st.file_uploader("选择您的 Excel/CSV 文件", type=['xlsx', 'csv'])
                         
                         if uploaded_file is not None:
                             try:
                                 # 自动识别文件类型
                                 if uploaded_file.name.endswith(('.csv', '.txt')):
                                     # 尝试使用GBK/utf-8解码，增强兼容性
                                     try:
                                         df_import = pd.read_csv(uploaded_file, encoding='utf-8')
                                     except UnicodeDecodeError:
                                         uploaded_file.seek(0) # 重置文件指针
                                         df_import = pd.read_csv(uploaded_file, encoding='gbk')

                                 else: # 默认为 Excel
                                     df_import = pd.read_excel(uploaded_file)
                                 
                                 st.success("文件读取成功！请预览数据并确认导入。")
                                 st.dataframe(df_import.head())
                                 
                                 if st.button("🚀 确认导入并写入数据库"):
                                     success, result = import_data_from_excel(df_import)
                                     if success:
                                         st.success(f"🎉 导入成功！共导入 {result} 条记录。")
                                         st.balloons()
                                         st.rerun()
                                     else:
                                         st.error(f"导入失败！请检查文件格式和列名。错误信息: {result}")
                                         
                             except Exception as e:
                                 st.error(f"读取文件失败，请确保格式正确且编码为 UTF-8 (如果是 CSV)。错误: {e}")


                     col_user, col_del, col_edit = st.columns(3)
                     
                     with col_user:
                         with st.expander("👤 用户管理"):
                             with st.form("add_user"):
                                 nu = st.text_input("用户名")
                                 npw = st.text_input("密码", type="password")
                                 ndn = st.text_input("中文名")
                                 nr = st.selectbox("角色", ['user', 'admin'])
                                 if st.form_submit_button("添加"):
                                     if add_new_user(nu, npw, nr, ndn):
                                         st.success("成功")
                                         st.rerun()
                                     else: st.error("失败")
                             st.dataframe(get_all_users(), hide_index=True)

                     with col_del:
                         with st.expander("🗑️ 删除记录"):
                             d_id = st.number_input("ID", min_value=1, key="del_id")
                             if st.button("删除"):
                                 delete_data(d_id)
                                 st.success("已删除")
                                 st.rerun()

                     with col_edit:
                         with st.expander("📝 修改基本信息(不含运费)"):
                             u_id = st.number_input("ID", min_value=1, key="edit_id")
                             if st.button("加载"):
                                 record = get_single_record(u_id) # 获取的是英文列名数据
                                 if record: 
                                     st.session_state['edit_record'] = record
                                     st.success("记录已加载，请修改并提交。")
                                 else: st.error("不存在")
                             
                             # 注意：这里 record['key'] 依然是英文数据库列名
                             if 'edit_record' in st.session_state and st.session_state['edit_record']['id'] == u_id:
                                 record = st.session_state['edit_record']
                                 with st.form("admin_edit"):
                                     nn = st.text_input("客户名", record['customer_name'])
                                     nph = st.text_input("电话", record['phone'])
                                     # 使用中文名作为 key，方便理解
                                     ns = st.selectbox(CRM_COL_MAP['source'], SOURCE_OPTIONS, index=SOURCE_OPTIONS.index(record['source']) if record['source'] in SOURCE_OPTIONS else 0)
                                     nshop = st.selectbox(CRM_COL_MAP['shop_name'], SHOP_OPTIONS, index=SHOP_OPTIONS.index(record['shop_name']) if record['shop_name'] in SHOP_OPTIONS else 0)
                                     nsite = st.selectbox(CRM_COL_MAP['site_type'], SITE_OPTIONS, index=SITE_OPTIONS.index(record['site_type']) if record['site_type'] in SITE_OPTIONS else 0)
                                     nup = st.number_input(CRM_COL_MAP['unit_price'], record['unit_price'])
                                     na = st.number_input(CRM_COL_MAP['area'], record['area'])
                                     nic = st.selectbox(CRM_COL_MAP['is_construction'], ["否","是"], index=["否","是"].index(record['is_construction']))
                                     ncf = st.number_input(CRM_COL_MAP['construction_fee'], record['construction_fee'])
                                     nmf = st.number_input(CRM_COL_MAP['material_fee'], record['material_fee'])
                                     nsf = st.number_input(CRM_COL_MAP['shipping_fee'], record.get('shipping_fee', 0.0))
                                     
                                     if st.form_submit_button("更新"):
                                         udata = {
                                             'customer_name': nn, 'phone': nph, 'source': ns,
                                             'shop_name': nshop, 'unit_price': nup, 'area': na, 
                                             'site_type': nsite, 'is_construction': nic, 
                                             'construction_fee': ncf, 'material_fee': nmf, 'shipping_fee': nsf,
                                             'status': record['status'], 'purchase_intent': record['purchase_intent']
                                         }
                                         admin_update_data(u_id, udata)
                                         del st.session_state['edit_record']
                                         st.success("已更新")
                                         st.rerun()
                     
                     # --- 修复功能 ---
                     st.markdown("---")
                     with st.expander("🚨 数据库维护工具"):
                         if st.button("🔄 修复单价/面积数据互换 (所有记录)"):
                             st.warning("⚠️ 警告：此操作将批量交换所有记录的单价和面积，并重算总金额（不含运费）。请确认执行！")
                             if st.button("🔥 确认执行修复操作"):
                                 rows = admin_fix_area_price_swap()
                                 st.success(f"🎉 修复完成！共影响 {rows} 条记录的单价、面积和总金额（不含运费）。")
                                 st.rerun()

        # 3. 销售分析页面 
        elif choice == "📈 销售分析看板":
            st.subheader("📊 经营数据大屏")
            
            # 侧边栏：目标设定
            st.sidebar.markdown("---")
            target_revenue = st.sidebar.number_input("🎯 本月业绩目标 (元)", min_value=10000, value=100000, step=5000, key="target_rev")
            # 🚨 新增面积目标
            target_area = st.sidebar.number_input("📐 本月面积目标 (㎡)", min_value=100.0, value=500.0, step=10.0, key="target_area")
            
            # 获取数据并转换为中文列名
            df = get_data(rename_cols=True) 
            
            if not df.empty:
                # 使用中文列名进行数值和日期处理
                df['预估总金额(元)'] = pd.to_numeric(df['预估总金额(元)'], errors='coerce').fillna(0)
                df['运费(元)'] = pd.to_numeric(df['运费(元)'], errors='coerce').fillna(0)
                df['施工费(元)'] = pd.to_numeric(df['施工费(元)'], errors='coerce').fillna(0)
                df['辅料费(元)'] = pd.to_numeric(df['辅料费(元)'], errors='coerce').fillna(0)
                df['平方数(㎡)'] = pd.to_numeric(df['平方数(㎡)'], errors='coerce').fillna(0)
                
                # 毛利计算 (🚨 总金额不含运费，所以毛利 = 总金额 - 施工费 - 辅料费)
                df['毛利'] = df['预估总金额(元)'] - df['施工费(元)'] - df['辅料费(元)'] 
                df['录入日期'] = pd.to_datetime(df['录入日期'], errors='coerce')
                df['月度'] = df['录入日期'].dt.strftime('%Y-%m')
                
                # 映射 '对接人' 列内容为中文名
                df['对接人'] = df['对接人'].map(user_map).fillna(df['对接人'])

                # --- 核心KPI ---
                current_month = datetime.date.today().strftime('%Y-%m')
                
                # 筛选已成交数据
                df_achieved = df[df['跟踪进度'] == '已完结/已收款'].copy()
                df_achieved['成交月'] = df_achieved['录入日期'].dt.strftime('%Y-%m')
                
                # 筛选本月已成交数据
                df_achieved_monthly = df_achieved[df_achieved['成交月'] == current_month]

                # 🚨 KPI 目标计算：仅使用已成交数据
                monthly_sales = df_achieved_monthly['预估总金额(元)'].sum()
                monthly_area = df_achieved_monthly['平方数(㎡)'].sum() 

                
                c1, c2, c3, c4, c5, c6 = st.columns(6) 
                c1.metric("💰 总销售额(不含运)", f"¥{df['预估总金额(元)'].sum():,.0f}")
                c2.metric("📈 总体毛利", f"¥{df['毛利'].sum():,.0f}", help="销售额(不含运费) - 施工费 - 辅料费")
                c3.metric("📏 总销售面积", f"{df['平方数(㎡)'].sum():,.0f} ㎡") 
                c4.metric("🚚 总运费", f"¥{df['运费(元)'].sum():,.0f}") 
                
                # 仅显示本月已成交的业绩
                c5.metric("✅ 本月成交额", f"¥{monthly_sales:,.0f}", delta=f"{monthly_sales - target_revenue:,.0f} (距目标)")
                c6.metric("📐 本月成交面积", f"{monthly_area:,.0f} ㎡", delta=f"{monthly_area - target_area:,.0f} (距目标)") 

                # --- 业绩达成进度条 ---
                st.write(f"**本月目标达成率 ({current_month}) (基于已成交)**")
                
                col_prog1, col_prog2 = st.columns(2)
                
                with col_prog1:
                    st.caption("金额目标达成率:")
                    progress_rev = min(monthly_sales / target_revenue, 1.0)
                    st.progress(progress_rev)
                    st.caption(f"目标: ¥{target_revenue:,.0f} | 当前: ¥{monthly_sales:,.0f} ({progress_rev*100:.1f}%)")
                
                with col_prog2:
                    st.caption("面积目标达成率:")
                    progress_area = min(monthly_area / target_area, 1.0)
                    st.progress(progress_area)
                    st.caption(f"目标: {target_area:,.0f} ㎡ | 当前: {monthly_area:,.0f} ㎡ ({progress_area*100:.1f}%)")


                st.markdown("---")
                
                # --- 销售龙虎榜 (基于实际成交金额) ---
                st.markdown("### 🏆 销售龙虎榜 (本月已成交数据)") 
                
                col_rank1, col_rank2 = st.columns(2)

                with col_rank1:
                    st.markdown("#### 💰 成交金额排名 (不含运费)")
                    if not df_achieved_monthly.empty:
                        # 分组时使用中文名列
                        leaderboard_data = df_achieved_monthly.groupby('对接人')['预估总金额(元)'].sum().reset_index()
                        leaderboard_data = leaderboard_data.sort_values('预估总金额(元)', ascending=False)
                        leaderboard_data.columns = ['👤 对接人', '💰 成交总额 (元)']

                        st.dataframe(
                            leaderboard_data.style.format({'💰 成交总额 (元)': '¥{:,.0f}'}),
                            hide_index=True,
                            use_container_width=True
                        )
                    else:
                        st.info("本月暂无已完结/已收款的成交记录。")

                with col_rank2:
                    st.markdown("#### 📐 销售面积排名 (已成交面积)")
                    if not df_achieved_monthly.empty:
                        # 分组时使用中文名列
                        area_leaderboard = df_achieved_monthly.groupby('对接人')['平方数(㎡)'].sum().reset_index()
                        area_leaderboard = area_leaderboard.sort_values('平方数(㎡)', ascending=False)
                        area_leaderboard.columns = ['👤 对接人', '📐 成交面积 (㎡)']

                        st.dataframe(
                            area_leaderboard.style.format({'📐 成交面积 (㎡)': '{:,.0f} ㎡'}),
                            hide_index=True,
                            use_container_width=True
                        )
                    else:
                        st.info("本月暂无已完结/已收款的成交记录。")
                
                st.markdown("---")


                # --- 第一排：趋势与利润 ---
                col_row1_1, col_row1_2 = st.columns(2)
                
                with col_row1_1:
                    # 1. 销售额(不含运)与毛利趋势 (使用所有记录)
                    monthly_trend = df.groupby('月度')[['预估总金额(元)', '毛利']].sum().reset_index()
                    fig_trend = px.line(monthly_trend, x='月度', y=['预估总金额(元)', '毛利'], markers=True, 
                                        title="📈 月度销售额(不含运费)与毛利趋势 (所有记录)", labels={'value':'金额', '月度':'月份', 'variable':'指标'})
                    st.plotly_chart(fig_trend, use_container_width=True)
                
                with col_row1_2:
                    # 2. 月度销售面积趋势图 (使用所有记录)
                    monthly_area_trend = df.groupby('月度')['平方数(㎡)'].sum().reset_index()
                    fig_area = px.bar(monthly_area_trend, x='月度', y='平方数(㎡)', text_auto='.0f',
                                      title="📐 月度销售面积趋势 (所有记录 - ㎡)", labels={'平方数(㎡)':'面积(㎡)', '月度':'月份'})
                    st.plotly_chart(fig_area, use_container_width=True)

                # --- 第二排：渠道与场地 ---
                col_row2_1, col_row2_2 = st.columns(2)
                
                with col_row2_1:
                    # 使用中文列名 (预估总金额不含运费)
                    shop_perf = df.groupby('店铺名称')['预估总金额(元)'].sum().reset_index().sort_values('预估总金额(元)', ascending=False)
                    fig_shop = px.bar(shop_perf, x='店铺名称', y='预估总金额(元)', text_auto='.2s', 
                                      title="🏪 各店铺业绩对比 (金额 - 不含运 - 所有记录)", color='店铺名称')
                    st.plotly_chart(fig_shop, use_container_width=True)

                with col_row2_2:
                    # 使用中文列名
                    site_perf = df.groupby('应用场地')['平方数(㎡)'].sum().reset_index().sort_values('平方数(㎡)', ascending=False).head(10)
                    fig_site = px.bar(site_perf, y='应用场地', x='平方数(㎡)', orientation='h', text_auto='.2s',
                                      title="🏟️ Top 10 销售场地类型 (面积 - 所有记录)", color_discrete_sequence=px.colors.qualitative.Pastel)
                    st.plotly_chart(fig_site, use_container_width=True)

                # --- 第三排：漏斗与来源 ---
                col_row3_1, col_row3_2 = st.columns(2)

                with col_row3_1:
                    # 使用中文列名
                    status_counts = df['跟踪进度'].value_counts().reset_index()
                    status_counts.columns = ['status', 'count']
                    sorter = STATUS_OPTIONS
                    status_counts['status'] = pd.Categorical(status_counts['status'], categories=sorter, ordered=True)
                    status_counts = status_counts.sort_values('status')
                    fig_funnel = px.funnel(status_counts, x='count', y='status', title="⏳ 客户跟进漏斗 (所有记录)", labels={'status':'进度'})
                    st.plotly_chart(fig_funnel, use_container_width=True)

                with col_row3_2:
                    # 使用中文列名
                    if '客户来源' in df.columns:
                        src_counts = df['客户来源'].value_counts().reset_index()
                        src_counts.columns = ['source', 'count']
                        fig_src = px.pie(src_counts, values='count', names='source', title="🌍 客户来源分布 (所有记录)", hole=0.4)
                        st.plotly_chart(fig_src, use_container_width=True)

            else:
                st.warning("暂无数据，请先录入销售信息。")

        # 4. 推广数据看板 
        elif choice == "🌐 推广数据看板":
            st.subheader("🌐 线上推广效果深度分析")
            
            df_promo = get_promo_data(rename_cols=True) # 获取中文列名数据
            
            with st.expander("➕ 录入推广数据 (按月/店铺/类型)"):
                with st.form("promo_entry"):
                    col_p1, col_p2, col_p3 = st.columns(3)
                    with col_p1:
                        d_val = st.date_input("推广月份 (选择该月任意一天即可)", value=datetime.date.today())
                        p_month = d_val.strftime("%Y-%m") # 自动转换为 2023-10 格式
                        p_shop = st.selectbox("店铺", SHOP_OPTIONS)
                        p_type = st.selectbox("推广类型", PROMO_TYPE_OPTIONS)
                    
                    with col_p2:
                        p_total_spend = st.number_input("总花费 (元)", min_value=0.0, step=10.0)
                        p_trans_spend = st.number_input("成交花费 (元)", min_value=0.0, step=10.0)
                        p_net_gmv = st.number_input("净成交额 (元)", min_value=0.0, step=100.0)
                        if p_total_spend > 0:
                            calc_roi = p_net_gmv / p_total_spend
                            st.caption(f"💡 自动计算净投产比(ROI): {calc_roi:.2f}")
                    
                    with col_p3:
                        p_net_roi = st.number_input("净投产比 (ROI)", min_value=0.0, step=0.1, value=(calc_roi if 'calc_roi' in locals() else 0.0))
                        p_cpa_net = st.number_input("每笔净成交花费 (元)", min_value=0.0, step=1.0)
                    
                    st.markdown("---")
                    col_p4, col_p5, col_p6 = st.columns(3)
                    with col_p4:
                        p_inquiry_count = st.number_input("询单量", min_value=0, step=1)
                    with col_p5:
                        p_inquiry_spend = st.number_input("询单花费 (元)", min_value=0.0, step=10.0)
                    with col_p6:
                        p_cpl_calc = p_inquiry_spend/p_inquiry_count if p_inquiry_count > 0 else 0.0
                        p_cpl = st.number_input("询单成本 (元/个)", min_value=0.0, step=1.0, value=p_cpl_calc)
                        if p_inquiry_count > 0:
                             st.caption(f"💡 自动计算询单成本: {p_cpl_calc:.2f}")
                    
                    p_note = st.text_area("备注及优化建议")
                    
                    if st.form_submit_button("✅ 提交数据"):
                        add_promo_data((p_month, p_shop, p_type, p_total_spend, p_trans_spend, p_net_gmv, 
                                        p_net_roi, p_cpa_net, p_inquiry_count, p_inquiry_spend, p_cpl, p_note))
                        st.success(f"已录入 {p_month} 数据！")
                        st.rerun()

            st.markdown("---")

            if not df_promo.empty:
                # df_promo 此时已经是中文列名
                num_cols = ['总花费(元)', '成交花费(元)', '净成交额(元)', '净投产比(ROI)', '每笔净成交花费(元)', '询单花费(元)', '询单成本(元/个)']
                for c in num_cols: 
                     # 🚨 修复 Key Error: 检查列是否存在
                     if c in df_promo.columns:
                         df_promo[c] = pd.to_numeric(df_promo[c], errors='coerce').fillna(0)
                if '询单量' in df_promo.columns:
                    df_promo['询单量'] = pd.to_numeric(df_promo['询单量'], errors='coerce').fillna(0).astype(int)

                st.markdown("### 1. 核心指标月度趋势")
                
                # 🚨 修复 Key Error: 聚合前检查列是否存在
                agg_cols = [c for c in ['总花费(元)', '净成交额(元)', '询单量'] if c in df_promo.columns]
                
                if '月份' in df_promo.columns and agg_cols:
                    df_summary = df_promo.groupby('月份')[agg_cols].sum().reset_index().sort_values('月份')
                else:
                    # 如果缺少关键列，则无法计算趋势
                    st.info("推广数据表缺少关键列，无法生成图表。请检查录入的数据。")
                    df_summary = pd.DataFrame(columns=['月份', '总花费(元)', '净成交额(元)', '询单量'])

                if '总花费(元)' in df_summary.columns:
                    df_summary['整体ROI'] = np.where(df_summary['总花费(元)']>0, df_summary['净成交额(元)']/df_summary['总花费(元)'], 0)
                    st.dataframe(df_summary.style.format({'整体ROI': '{:.2f}', '总花费(元)': '{:,.0f}', '净成交额(元)': '{:,.0f}'}), hide_index=True)
                else:
                     st.info("数据不完整，无法显示摘要。")

                if not df_summary.empty and '总花费(元)' in df_summary.columns and '净成交额(元)' in df_summary.columns:
                    col_c1, col_c2 = st.columns(2)
                    with col_c1:
                        fig1 = px.bar(df_summary, x='月份', y=['净成交额(元)', '总花费(元)'], barmode='group', 
                                      title='投入产出对比 (GMV vs Cost)', labels={'value':'金额','variable':'指标'})
                        st.plotly_chart(fig1, use_container_width=True)
                    
                    with col_c2:
                        fig2 = px.line(df_summary, x='月份', y='整体ROI', title='整体净投产比 (ROI) 趋势', markers=True)
                        st.plotly_chart(fig2, use_container_width=True)

                    st.markdown("### 2. 深度运营分析")
                    col_c3, col_c4 = st.columns(2)
                    
                    with col_c3:
                        df_shop = df_promo.groupby('店铺').agg({'总花费(元)':'sum', '净成交额(元)':'sum'}).reset_index()
                        df_shop['ROI'] = np.where(df_shop['总花费(元)']>0, df_shop['净成交额(元)']/df_shop['总花费(元)'], 0)
                        fig3 = px.bar(df_shop, x='店铺', y='ROI', color='店铺', title='各店铺投产比 (ROI) 对比', text_auto='.2f')
                        st.plotly_chart(fig3, use_container_width=True)
                    
                    with col_c4:
                        if '询单成本(元/个)' in df_promo.columns:
                            df_cpl = df_promo.groupby('月份')['询单成本(元/个)'].mean().reset_index()
                            fig4 = px.line(df_cpl, x='月份', y='询单成本(元/个)', title='平均询单成本 (CPL) 趋势', markers=True)
                            st.plotly_chart(fig4, use_container_width=True)
                        else:
                            st.info("缺少 '询单成本(元/个)' 列，无法绘制 CPL 趋势图。")

                    st.markdown("### 3. 数据明细表")
                    st.dataframe(df_promo, hide_index=True, use_container_width=True)
                
            else:
                st.info("暂无推广数据，请先录入。")

if __name__ == '__main__':
    main()