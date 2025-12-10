import streamlit as st
import pandas as pd
import sqlite3
import datetime
import plotly.express as px
import numpy as np
import io

# --- 配置与数据初始化 ---
DB_FILE = 'crm_data.db'
PROMO_DB_FILE = 'promo_data.db'
USER_DB_FILE = 'user_management.db' # 新增：用户管理数据库
DAYS_FOR_TRANSFER = 20 

# 1. 初始用户账号配置 (只用于第一次数据库初始化)
# IMPORTANT: 'display_name' 字段是新增的中文名
INITIAL_USERS = {
    'admin': {'password': 'admin123', 'role': 'admin', 'display_name': '超级管理员'},
    'zhaoxiaoan': {'password': 'zhaoxiaoan123', 'role': 'admin', 'display_name': '赵小安'},
    'liqiufang': {'password': '123', 'role': 'user', 'display_name': '李秋芳'}, 
    'fanqiuju': {'password': '123', 'role': 'user', 'display_name': '范秋菊'},
    'zhoumengke': {'password': '123', 'role': 'user', 'display_name': '周梦珂'},
}

# 2. 下拉选项配置 (保持不变)
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
SOURCE_OPTIONS = ["自然进店", "抖音/快手推广", "老客户转介绍", "地推/线下活动", "招标/公海", "其他"]
PROMO_TYPE_OPTIONS = ["成交收费", "成交加扣"]
PROMO_PLAN_OPTIONS = ["运动综合品推广", "运动品专项推广", "竞价推广", "关键词推广", "新品推广", "其他"]


# --- 数据库函数 (用户管理) ---
def init_user_db():
    conn = sqlite3.connect(USER_DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY,
        password TEXT,
        role TEXT,
        display_name TEXT
    )''')
    conn.commit()
    
    # 检查是否需要迁移初始用户
    c.execute("SELECT COUNT(*) FROM users")
    if c.fetchone()[0] == 0:
        st.info("首次运行，正在导入初始用户数据...")
        for username, data in INITIAL_USERS.items():
            c.execute("INSERT INTO users VALUES (?, ?, ?, ?)", 
                      (username, data['password'], data['role'], data['display_name']))
        conn.commit()
        st.success("初始用户数据导入完成。")
        
    conn.close()

def get_all_users():
    conn = sqlite3.connect(USER_DB_FILE)
    df = pd.read_sql_query("SELECT username, role, display_name FROM users", conn)
    conn.close()
    return df

def get_user_info(username):
    conn = sqlite3.connect(USER_DB_FILE)
    c = conn.cursor()
    c.execute("SELECT password, role, display_name FROM users WHERE username=?", (username,))
    result = c.fetchone()
    conn.close()
    if result:
        return {'password': result[0], 'role': result[1], 'display_name': result[2]}
    return None

def add_new_user(username, password, role, display_name):
    conn = sqlite3.connect(USER_DB_FILE)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO users VALUES (?, ?, ?, ?)", (username, password, role, display_name))
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        conn.close()
        return False # 用户名已存在

# 获取用户名到中文名的映射
def get_user_map():
    df = get_all_users()
    return df.set_index('username')['display_name'].to_dict()

# --- 数据库函数 (CRM 客户数据) ---
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        sales_rep TEXT,           -- 存储 username
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

def add_data(data):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''INSERT INTO sales (
        date, sales_rep, customer_name, phone, source, shop_name, unit_price, area, 
        site_type, status, is_construction, construction_fee, material_fee, 
        purchase_intent, total_amount, follow_up_history, sample_no, order_no,
        last_follow_up_date, next_follow_up_date 
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', data)
    conn.commit()
    conn.close()

def get_data():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM sales", conn)
    conn.close()
    return df

def get_single_record(record_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT * FROM sales WHERE id=?", (record_id,))
    columns = [desc[0] for desc in c.description]
    record = c.fetchone()
    conn.close()
    if record:
        return dict(zip(columns, record))
    return None

def admin_update_data(record_id, data):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    total_amount = data['unit_price'] * data['area'] + data['construction_fee'] + data['material_fee']
    
    c.execute('''UPDATE sales SET
        customer_name=?, phone=?, source=?, shop_name=?, unit_price=?, area=?, 
        site_type=?, is_construction=?, construction_fee=?, material_fee=?, 
        total_amount=?
        WHERE id=?''', (
        data['customer_name'], data['phone'], data['source'], data['shop_name'], data['unit_price'], data['area'], 
        data['site_type'], data['is_construction'], data['construction_fee'], data['material_fee'], 
        total_amount, record_id
    ))
    conn.commit()
    conn.close()
    update_follow_up(record_id, "[管理员修改]: 客户基本信息 (单价/面积/费用) 已更新，请重新核对。", 
                     datetime.date.today().isoformat(), data['status'], data['purchase_intent'])

def delete_data(record_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM sales WHERE id=?", (record_id,))
    conn.commit()
    conn.close()

def transfer_sales_rep(record_id, new_rep_username):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    log = f"\n[{datetime.date.today()}] 系统自动转交：客户超期，已转交给 {get_user_info(new_rep_username)['display_name']}"
    c.execute("UPDATE sales SET sales_rep=?, status='转交管理', last_follow_up_date=?, follow_up_history=follow_up_history || ? WHERE id=?", 
              (new_rep_username, datetime.date.today().isoformat(), log, record_id))
    conn.commit()
    conn.close()

def update_follow_up(record_id, new_log, next_date, new_status, new_intent):
    conn = sqlite3.connect(DB_FILE)
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
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT sales_rep FROM sales WHERE customer_name=? OR (phone!='' AND phone=?)", (name, phone))
    result = c.fetchone()
    conn.close()
    return result[0] if result else None

# --- 数据库函数 (推广数据) ---
def init_promo_db():
    conn = sqlite3.connect(PROMO_DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS promotions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        month TEXT,
        shop TEXT,
        promo_plan TEXT,
        promo_type TEXT,
        total_revenue REAL,
        revenue_commission REAL,
        promo_expense REAL,
        revenue_ratio REAL,
        net_revenue REAL,
        order_count INTEGER,
        sample_count REAL,
        avg_cost REAL,
        note TEXT
    )''')
    conn.commit()
    conn.close()

def add_promo_data(data):
    conn = sqlite3.connect(PROMO_DB_FILE)
    c = conn.cursor()
    c.execute('''INSERT INTO promotions (
        month, shop, promo_plan, promo_type, total_revenue, revenue_commission, 
        promo_expense, revenue_ratio, net_revenue, order_count, sample_count, 
        avg_cost, note
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', data)
    conn.commit()
    conn.close()

def get_promo_data():
    conn = sqlite3.connect(PROMO_DB_FILE)
    df = pd.read_sql_query("SELECT * FROM promotions", conn)
    conn.close()
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
    st.set_page_config(page_title="CRM全功能版", layout="wide")
    init_user_db() # 初始化用户数据库
    init_db()
    init_promo_db()

    if check_password():
        user_role = st.session_state["role"]
        current_user = st.session_state["user_now"]
        current_display_name = st.session_state["display_name"]
        
        # 获取用户名和中文名映射，用于表格显示
        user_map = get_user_map()
        
        st.sidebar.title(f"👤 {current_display_name}")
        menu = ["📝 新增销售记录", "📊 数据追踪与查看", "📈 销售分析看板", "🌐 推广数据看板"]
        choice = st.sidebar.radio("菜单", menu)
        
        # --- 侧边栏：数据导出 (保持不变) ---
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 💾 数据备份")
        
        if st.sidebar.button("下载客户数据 (Excel)"):
            df_export = get_data()
            if not df_export.empty:
                # 转换 sales_rep 为中文名
                df_export['sales_rep'] = df_export['sales_rep'].map(user_map).fillna(df_export['sales_rep'])
                output = io.BytesIO()
                df_export['total_amount'] = (df_export['unit_price'] * df_export['area']) + df_export['construction_fee'] + df_export['material_fee']
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_export.to_excel(writer, index=False, sheet_name='Sheet1')
                excel_data = output.getvalue()
                st.sidebar.download_button(label="📥 客户数据备份", data=excel_data, file_name=f'CRM_Customer_Backup_{datetime.date.today()}.xlsx', mime='application/vnd.ms-excel')
            else:
                st.sidebar.warning("暂无客户数据可导出")
        
        if st.sidebar.button("下载推广数据 (Excel)"):
            df_promo_export = get_promo_data()
            if not df_promo_export.empty:
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_promo_export.to_excel(writer, index=False, sheet_name='Sheet1')
                excel_data = output.getvalue()
                st.sidebar.download_button(label="📥 推广数据备份", data=excel_data, file_name=f'CRM_Promo_Backup_{datetime.date.today()}.xlsx', mime='application/vnd.ms-excel')
            else:
                st.sidebar.warning("暂无推广数据可导出")


        # 1. 新增记录页面 (CRM)
        if choice == "📝 新增销售记录":
             st.subheader("📝 客户信息录入")
             with st.form("entry_form", clear_on_submit=True):
                 col1, col2, col3 = st.columns(3)
                 with col1:
                     date_val = st.date_input("日期", datetime.date.today())
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
                     # 显示中文名，但实际存储的是 username
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

                 submitted = st.form_submit_button("✅ 提交录入")

                 if submitted:
                     if customer_name == "":
                         st.warning("⚠️ 客户名称不能为空")
                     else:
                         existing_rep = check_customer_exist(customer_name, phone)
                         if existing_rep:
                             # 显示已存在客户的中文名
                             rep_display_name = user_map.get(existing_rep, existing_rep)
                             st.error(f"❌ 录入失败！该客户已存在，目前由 **{rep_display_name}** 负责。请勿重复录入。")
                         else:
                             calc_total = (unit_price * area) + const_fee + mat_fee
                             # 跟进历史中记录中文名
                             log_entry = f"[{datetime.date.today()} {current_display_name}]: 首次录入。{first_remark}"
                             
                             data_tuple = (
                                 date_val, current_user, customer_name, phone, source, shop_name, unit_price, area,
                                 site_type, status, is_const, const_fee, mat_fee,
                                 purchase_intent, calc_total, log_entry, sample_no, order_no,
                                 str(last_fup), str(next_fup)
                             )
                             add_data(data_tuple)
                             st.success(f"🎉 客户 {customer_name} 录入成功！")


        # 2. 数据查看页面 (CRM)
        elif choice == "📊 数据追踪与查看":
             st.subheader("📋 客户追踪列表")
             df = get_data()
             
             # --- 快速追加跟进记录功能 ---
             with st.expander("➕ 快速追加跟进记录 (无需进表单修改)"):
                 col_up1, col_up2 = st.columns([1, 2])
                 with col_up1:
                     if not df.empty:
                         # 这里的下拉列表显示中文名
                         df['display_rep'] = df['sales_rep'].map(user_map).fillna(df['sales_rep'])
                         customer_id_map = {f"{row['id']} - {row['customer_name']} ({row['display_rep']})": row['id'] for index, row in df.iterrows()}
                         selected_customer_label = st.selectbox("选择客户 ID 和名称", list(customer_id_map.keys()))
                         up_id = customer_id_map.get(selected_customer_label, None)
                     else:
                         up_id = st.number_input("输入客户 ID", min_value=1, step=1)
                 with col_up2:
                     up_content = st.text_input("本次跟进情况 (例如：客户说价格还能谈)")
                 
                 col_up3, col_up4, col_up5 = st.columns(3)
                 with col_up3:
                     up_next_date = st.date_input("下次跟进时间", datetime.date.today() + datetime.timedelta(days=3))
                 with col_up4:
                     up_status = st.selectbox("更新进度状态", STATUS_OPTIONS, key="up_stat")
                 with col_up5:
                     up_intent = st.selectbox("更新购买意向", INTENT_OPTIONS, key="up_int")
                 
                 if st.button("🚀 提交跟进更新"):
                     if up_id is None:
                          st.error("请先录入客户数据。")
                     elif not df.empty and up_id in df['id'].values:
                        record_rep = df[df['id'] == up_id]['sales_rep'].values[0]
                        if user_role == 'admin' or record_rep == current_user:
                            # 记录时使用中文名
                            new_log = f"[{datetime.date.today()} {current_display_name}]: {up_content}"
                            update_follow_up(up_id, new_log, str(up_next_date), up_status, up_intent)
                            st.success("跟进记录已追加！")
                            st.rerun()
                        else:
                            st.error("您没有权限更新此客户。")
                     else:
                         st.error("ID 不存在")

             st.markdown("---")
             
             # --- 提醒与表格 ---
             if not df.empty:
                 df['next_follow_up_date'] = pd.to_datetime(df['next_follow_up_date'], errors='coerce')
                 df['last_follow_up_date'] = pd.to_datetime(df['last_follow_up_date'], errors='coerce')
                 df['date'] = pd.to_datetime(df['date'], errors='coerce') 
                 today = datetime.date.today()
                 
                 df['days_since_fup'] = (pd.to_datetime(today) - df['last_follow_up_date']).dt.days
                 
                 overdue = df[(df['status'] != '已完结/已收款') & (df['days_since_fup'] > DAYS_FOR_TRANSFER)]
                 if user_role == 'admin' and not overdue.empty:
                     st.error(f"⚠️ 管理员注意：有 {len(overdue)} 个客户超 {DAYS_FOR_TRANSFER} 天未跟进！")
                     if st.button("🔥 一键接管所有超期客户"):
                         for pid in overdue['id'].values:
                             transfer_sales_rep(pid, 'admin')
                         st.success("已全部转入管理员名下")
                         st.rerun()

                 my_reminders = df[
                     (df['next_follow_up_date'].dt.date <= today) & 
                     (df['status'] != '已完结/已收款') &
                     (df['sales_rep'] == current_user)
                 ]
                 if not my_reminders.empty:
                     st.warning(f"🔔 {current_display_name}，您今天有 {len(my_reminders)} 个待办跟进！")

                 # 表格显示过滤区域
                 col_filter_month, col_filter_rep, col_search = st.columns(3)
                 
                 with col_filter_month:
                     df['year_month'] = df['date'].dt.strftime('%Y年%m月')
                     month_options = ['全部月份'] + sorted(df['year_month'].unique().tolist(), reverse=True)
                     filter_month = st.selectbox("🗓️ 录入月份筛选", month_options)
                     
                 with col_filter_rep:
                     # 筛选栏显示中文名
                     rep_display_options = ['全部'] + list(user_map.values())
                     filter_rep_display = st.selectbox("👤 对接人筛选", rep_display_options)
                     
                     # 将选中的中文名转换回 username 进行筛选
                     if filter_rep_display != '全部':
                         filtered_username = next( (k for k, v in user_map.items() if v == filter_rep_display), None)
                     else:
                         filtered_username = None

                 with col_search:
                     search_term = st.text_input("🔍 搜客户、电话或店铺")

                 df_show = df.copy()
                 
                 # 应用筛选
                 if filter_month != '全部月份':
                     df_show = df_show[df_show['year_month'] == filter_month]
                     
                 if filtered_username:
                     df_show = df_show[df_show['sales_rep'] == filtered_username]
                     
                 if search_term:
                     df_show = df_show[
                         df_show['customer_name'].astype(str).str.contains(search_term, case=False) |
                         df_show['phone'].astype(str).str.contains(search_term, case=False) |
                         df_show['shop_name'].astype(str).str.contains(search_term, case=False)
                     ]

                 # ❗ 将 sales_rep 字段替换为中文显示名 ❗
                 df_show['sales_rep'] = df_show['sales_rep'].map(user_map).fillna(df_show['sales_rep'])
                 
                 # 格式化日期显示
                 st.dataframe(
                     df_show.drop(columns=['year_month']),
                     hide_index=True, 
                     use_container_width=True,
                     column_config={
                         "sales_rep": st.column_config.TextColumn("👤 对接人"), # 字段名不变，内容已替换
                         "follow_up_history": st.column_config.TextColumn("📜 跟进历史 (详细)", width="large"),
                         "last_follow_up_date": st.column_config.DateColumn("上次跟进"),
                         "next_follow_up_date": st.column_config.DateColumn("计划下次"),
                     }
                 )

                 # --- 管理员功能区 ---
                 if user_role == 'admin':
                     st.markdown("---")
                     st.subheader("🛠️ 管理员操作区 (Admin Only)")
                     
                     col_user, col_del, col_edit = st.columns(3)

                     # 🆕 用户管理功能
                     with col_user:
                         with st.expander("👤 用户管理"):
                             st.markdown("**新增用户**")
                             with st.form("add_user_form", clear_on_submit=True):
                                 new_username = st.text_input("用户名 (Login ID)")
                                 new_password = st.text_input("密码", type="password")
                                 new_display_name = st.text_input("中文名 (显示名称)")
                                 new_role = st.selectbox("角色", ['user', 'admin'])
                                 
                                 if st.form_submit_button("➕ 添加用户"):
                                     if new_username and new_password and new_display_name:
                                         if add_new_user(new_username, new_password, new_role, new_display_name):
                                             st.success(f"用户 {new_display_name} ({new_username}) 添加成功！")
                                             st.rerun()
                                         else:
                                             st.error("用户名已存在，请更换。")
                                     else:
                                         st.warning("所有字段不能为空。")
                             
                             st.markdown("**现有用户列表**")
                             st.dataframe(get_all_users(), hide_index=True, use_container_width=True)


                     # 删除功能 (保持不变)
                     with col_del:
                         with st.expander("🗑️ 删除记录"):
                             d_id = st.number_input("输入要删除的客户 ID", min_value=1, key="del_id", step=1)
                             if st.button("🚨 确认永久删除"):
                                 delete_data(d_id)
                                 st.success(f"客户 ID {d_id} 已删除。")
                                 st.rerun()

                     # 修改功能 (保持不变)
                     with col_edit:
                         with st.expander("📝 修改客户基本信息 (更新计算依据)"):
                             u_id = st.number_input("输入要修改的客户 ID", min_value=1, key="edit_id", step=1)
                             
                             if st.button("🔍 加载该 ID 信息"):
                                 record = get_single_record(u_id)
                                 if record:
                                     st.session_state['edit_record'] = record
                                 else:
                                     st.error("该 ID 客户不存在。")
                             
                             if 'edit_record' in st.session_state and st.session_state['edit_record']['id'] == u_id:
                                 record = st.session_state['edit_record']
                                 
                                 with st.form("admin_edit_form"):
                                     # 显示中文名
                                     rep_display = user_map.get(record['sales_rep'], record['sales_rep'])
                                     st.caption(f"正在修改客户: {record['customer_name']} (原对接人: {rep_display})")
                                     
                                     # ... (其余修改表单保持不变) ...
                                     new_name = st.text_input("客户名称", value=record['customer_name'])
                                     new_phone = st.text_input("联系电话", value=record['phone'])
                                     new_source = st.selectbox("客户来源", SOURCE_OPTIONS, index=SOURCE_OPTIONS.index(record['source']))
                                     
                                     new_shop_name = st.selectbox("店铺名字", SHOP_OPTIONS, index=SHOP_OPTIONS.index(record['shop_name']))
                                     new_site_type = st.selectbox("应用场地", SITE_OPTIONS, index=SITE_OPTIONS.index(record['site_type']))
                                     
                                     new_unit_price = st.number_input("单价 (元/㎡)", value=record['unit_price'], min_value=0.0, step=0.1)
                                     new_area = st.number_input("平方数 (㎡)", value=record['area'], min_value=0.0, step=0.1)
                                     
                                     new_is_const = st.selectbox("是否施工", ["否", "是"], index=["否", "是"].index(record['is_construction']))
                                     new_const_fee = st.number_input("施工费 (元)", value=record['construction_fee'], min_value=0.0, step=100.0)
                                     new_mat_fee = st.number_input("辅料费用 (元)", value=record['material_fee'], min_value=0.0, step=50.0)

                                     current_status = record['status']
                                     current_intent = record['purchase_intent']

                                     submitted_update = st.form_submit_button("💾 确认更新基本信息")

                                     if submitted_update:
                                         update_data = {
                                             'customer_name': new_name, 'phone': new_phone, 'source': new_source,
                                             'shop_name': new_shop_name, 'unit_price': new_unit_price, 'area': new_area, 
                                             'site_type': new_site_type, 'is_construction': new_is_const, 
                                             'construction_fee': new_const_fee, 'material_fee': new_mat_fee,
                                             'status': current_status, 'purchase_intent': current_intent
                                         }
                                         admin_update_data(u_id, update_data)
                                         del st.session_state['edit_record']
                                         st.success(f"客户 ID {u_id} 的基本信息已更新，分析数据已同步！")
                                         st.rerun()

        # 3. 销售分析页面 (CRM)
        elif choice == "📈 销售分析看板":
            st.subheader("📊 经营数据大屏")
            df = get_data()
            if not df.empty:
                df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce').fillna(0)
                
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("💰 销售总额", f"¥{df['total_amount'].sum():,.0f}")
                c2.metric("📦 订单总量", len(df))
                
                closed_count = len(df[df['status']=='已完结/已收款'])
                total_count = len(df)
                completion_rate = closed_count / total_count * 100 if total_count > 0 else 0
                
                c3.metric("🔥 成交率", f"{completion_rate:.1f}%")
                c4.metric("🛑 流失数", len(df[df['purchase_intent']=='流失']))

                st.markdown("---")
                
                c_chart1, c_chart2 = st.columns(2)
                with c_chart1:
                    # 销售龙虎榜：使用中文名进行分组和显示
                    df['display_rep'] = df['sales_rep'].map(user_map).fillna(df['sales_rep'])
                    rep_perf = df.groupby('display_rep')['total_amount'].sum().reset_index().sort_values('total_amount', ascending=False)
                    fig = px.bar(rep_perf, x='display_rep', y='total_amount', text_auto=True, title="🏆 销售龙虎榜 (按金额)", color='display_rep', labels={'display_rep': '对接人'})
                    st.plotly_chart(fig, use_container_width=True)
                
                with c_chart2:
                    if 'source' in df.columns:
                        src_counts = df['source'].value_counts().reset_index()
                        src_counts.columns = ['source', 'count']
                        fig2 = px.pie(src_counts, values='count', names='source', title="🌍 客户来源分布", hole=0.4)
                        st.plotly_chart(fig2, use_container_width=True)

        # 4. 推广数据看板 (保持不变)
        elif choice == "🌐 推广数据看板":
            st.subheader("🌐 线上推广效果总览")
            
            df_promo = get_promo_data()
            
            with st.expander("➕ 录入新的推广数据"):
                # ... (录入表单保持不变) ...
                with st.form("promo_entry_form", clear_on_submit=True):
                    col_p1, col_p2, col_p3 = st.columns(3)
                    with col_p1:
                        month_val = st.number_input("月份 (如 9)", min_value=1, max_value=12, step=1, key="p_month")
                        shop = st.selectbox("店铺", SHOP_OPTIONS)
                        promo_plan = st.selectbox("推广计划", PROMO_PLAN_OPTIONS)
                        promo_type = st.selectbox("推广类型", PROMO_TYPE_OPTIONS)
                    
                    with col_p2:
                        total_revenue = st.number_input("总成交额 (元)", min_value=0.0, step=10.0)
                        revenue_commission = st.number_input("成交收佣 (元)", min_value=0.0, step=10.0)
                        promo_expense = st.number_input("推广总花费 (元)", min_value=0.0, step=10.0)
                        revenue_ratio = st.number_input("净营收比 (仅作记录)", min_value=0.0, step=0.1)
                    
                    with col_p3:
                        order_count = st.number_input("询单量", min_value=0, step=1)
                        sample_count = st.number_input("寄样费 (元)", min_value=0.0, step=10.0)
                        avg_cost = st.number_input("平均成本 (元)", min_value=0.0, step=0.01)
                        
                    note = st.text_area("备注与优化建议")
                    
                    promo_submitted = st.form_submit_button("✅ 提交推广数据")
                    
                    if promo_submitted:
                        net_revenue = total_revenue - revenue_commission
                        data_tuple = (
                            str(month_val), shop, promo_plan, promo_type, total_revenue, revenue_commission, 
                            promo_expense, revenue_ratio, net_revenue, order_count, sample_count, 
                            avg_cost, note
                        )
                        add_promo_data(data_tuple)
                        st.success(f"🎉 {month_val}月 {shop} 推广数据录入成功！")
                        st.rerun()

            st.markdown("---")

            if not df_promo.empty:
                df_promo['total_revenue'] = pd.to_numeric(df_promo['total_revenue'], errors='coerce').fillna(0)
                df_promo['promo_expense'] = pd.to_numeric(df_promo['promo_expense'], errors='coerce').fillna(0)
                df_promo['order_count'] = pd.to_numeric(df_promo['order_count'], errors='coerce').fillna(0).astype(int)
                df_promo['net_revenue'] = pd.to_numeric(df_promo['net_revenue'], errors='coerce').fillna(0)


                df_promo_summary = df_promo.groupby('month').agg({
                    'total_revenue': 'sum',
                    'promo_expense': 'sum',
                    'order_count': 'sum',
                    'net_revenue': 'sum'
                }).reset_index().sort_values('month')
                
                df_promo_summary['ROI'] = (df_promo_summary['net_revenue'] / df_promo_summary['promo_expense']) * 100
                df_promo_summary.rename(columns={
                    'total_revenue': '总成交额',
                    'promo_expense': '推广总花费',
                    'order_count': '总询单量',
                    'net_revenue': '净收佣利润'
                }, inplace=True)
                
                st.markdown("### 月度核心指标")
                st.dataframe(df_promo_summary, hide_index=True)
                
                fig_rev = px.bar(df_promo_summary, x='month', y=['总成交额', '推广总花费'], 
                                 title='月度成交额与推广费用对比', barmode='group')
                st.plotly_chart(fig_rev, use_container_width=True)

                fig_net = px.line(df_promo_summary, x='month', y='净收佣利润', 
                                  title='月度净收佣利润趋势', markers=True)
                st.plotly_chart(fig_net, use_container_width=True)

                st.markdown("### 推广记录明细")
                st.dataframe(df_promo, hide_index=True, use_container_width=True)
                
            else:
                st.info("请先录入推广数据。")

if __name__ == '__main__':
    main()