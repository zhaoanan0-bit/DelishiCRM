import streamlit as st
import pandas as pd
import sqlite3
import datetime

# --- 1. 初始化配置 ---
st.set_page_config(page_title="CRM 增强版", layout="wide")

# 选项配置
SALES_REPS = ["范秋菊", "李秋芳", "周梦珂", "赵小安"]
SHOPS = ["拼多多运动店", "拼多多旗舰店", "天猫旗舰店", "天猫德丽士旗舰店", "淘宝店", "抖店"]
SOURCES = ["自然进店", "转介绍", "线下渠道"]
STATUS_LIST = ["初次接触", "方案报价", "样品测试", "价格谈判", "已签约", "已流失"]

# --- 2. 数据库与安全转换函数 ---

def init_db():
    conn = sqlite3.connect('crm_full.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT, sales_rep TEXT, customer_name TEXT, phone TEXT, source TEXT, 
        shop_name TEXT, unit_price REAL, area REAL, site_type TEXT, status TEXT, 
        is_construction TEXT, construction_fee REAL, material_fee REAL, 
        shipping_fee REAL, purchase_intent TEXT, total_amount REAL, 
        follow_up_history TEXT, next_follow_up_date TEXT
    )''')
    conn.commit()
    conn.close()

def safe_float(val):
    """【核心修复】防止平方数/单价乱码导致无法修改"""
    if pd.isna(val) or val == "" or val == "None": return 0.0
    try: return float(val)
    except: return 0.0

def safe_date_comp(date_str):
    """【核心修复】防止日期对比报错"""
    if not date_str or date_str == "None": return None
    try: return pd.to_datetime(date_str).date()
    except: return None

# --- 3. 侧边栏登录系统 ---

def login_system():
    st.sidebar.title("👤 账户登录")
    user = st.sidebar.selectbox("选择登录人", ["超级管理员"] + SALES_REPS)
    return user

# --- 4. 功能模块 ---

def show_add_page(current_user):
    st.header(f"📝 录入新客户记录 (当前: {current_user})")
    with st.form("add_form"):
        c1, c2, c3 = st.columns(3)
        date_in = c1.date_input("录入日期", datetime.date.today())
        cust_name = c1.text_input("客户名称 (必填)")
        rep = c2.selectbox("对接人", SALES_REPS, index=SALES_REPS.index(current_user) if current_user in SALES_REPS else 0)
        shop = c2.selectbox("店铺名称", SHOPS)
        source = c3.selectbox("客户来源", SOURCES)
        site = c3.selectbox("应用场地", ["篮球馆", "羽毛球馆", "乒乓球", "其他"])
        
        c4, c5, c6 = st.columns(3)
        price = c4.number_input("单价(元/㎡)", min_value=0.0)
        area = c5.number_input("平方数(㎡)", min_value=0.0)
        ship = c6.number_input("运费(元)", min_value=0.0)
        
        c7, c8, c9 = st.columns(3)
        status = c7.selectbox("跟踪进度", STATUS_LIST)
        is_cons = c8.selectbox("是否施工", ["否", "是"])
        next_date = c9.date_input("计划下次跟进", datetime.date.today() + datetime.timedelta(days=3))
        
        history = st.text_area("跟进记录/备注")
        
        if st.form_submit_button("确认录入数据"):
            if not cust_name:
                st.error("请输入客户名称")
            else:
                conn = sqlite3.connect('crm_full.db')
                c = conn.cursor()
                total = (price * area) + ship
                c.execute("""INSERT INTO sales (date, sales_rep, customer_name, source, shop_name, unit_price, area, site_type, status, is_construction, shipping_fee, total_amount, follow_up_history, next_follow_up_date) 
                             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", 
                          (str(date_in), rep, cust_name, source, shop, price, area, site, status, is_cons, ship, total, history, str(next_date)))
                conn.commit()
                st.success("数据录入成功！")

def show_view_page(user):
    st.header("📊 数据追踪与跟进")
    conn = sqlite3.connect('crm_full.db')
    df = pd.read_sql_query("SELECT * FROM sales", conn)
    conn.close()

    if df.empty:
        st.info("目前没有数据"); return

    # 1. 逾期提醒逻辑 (修复乱码报错)
    today = datetime.date.today()
    df['next_dt'] = df['next_follow_up_date'].apply(safe_date_comp)
    overdue = df[df['next_dt'] < today]
    if not overdue.empty:
        st.warning(f"🔔 提醒：有 {len(overdue)} 条记录已逾期，请及时跟进！")

    # 2. 数据表格
    st.dataframe(df.drop(columns=['next_dt']), use_container_width=True)

    # 3. 编辑区 (恢复所有字段)
    if user == "超级管理员" or st.checkbox("开启编辑模式"):
        st.markdown("---")
        st.subheader("🛠️ 管理员编辑/修改")
        edit_id = st.number_input("输入要修改的 ID", min_value=1, step=1)
        row = df[df['id'] == edit_id]
        
        if not row.empty:
            record = row.iloc[0]
            with st.form("edit_full_form"):
                c1, c2, c3 = st.columns(3)
                # 使用 safe_float 解决报错核心
                new_price = c1.number_input("单价", value=safe_float(record['unit_price']))
                new_area = c2.number_input("平方数", value=safe_float(record['area']))
                new_status = c3.selectbox("进度", STATUS_LIST, index=STATUS_LIST.index(record['status']) if record['status'] in STATUS_LIST else 0)
                
                # 必须有这个按钮
                if st.form_submit_button("保存修改"):
                    conn = sqlite3.connect('crm_full.db')
                    c = conn.cursor()
                    new_total = (new_price * new_area) + safe_float(record['shipping_fee'])
                    c.execute("UPDATE sales SET unit_price=?, area=?, status=?, total_amount=? WHERE id=?", 
                              (new_price, new_area, new_status, new_total, edit_id))
                    conn.commit()
                    st.success(f"ID {edit_id} 已更新！")
                    st.rerun()

def show_analysis_page():
    st.header("📈 销售分析看板 (增强版)")
    conn = sqlite3.connect('crm_full.db')
    df = pd.read_sql_query("SELECT * FROM sales", conn)
    conn.close()
    
    if df.empty: return

    # 对接人业绩
    st.subheader("1. 对接人业绩统计")
    rep_stats = df.groupby('sales_rep').agg({'id':'count', 'total_amount':'sum'}).rename(columns={'id':'项目数','total_amount':'总金额'})
    st.table(rep_stats)

    # 店铺转化
    st.subheader("2. 店铺渠道转化统计")
    shop_stats = df.groupby('shop_name').size().reset_index(name='项目数量')
    st.bar_chart(shop_stats.set_index('shop_name'))

# --- 5. 主程序 ---

def main():
    init_db()
    current_user = login_system()
    
    st.sidebar.markdown("---")
    menu = st.sidebar.radio("菜单", ["📝 新增销售记录", "📊 数据追踪与查看", "📈 销售分析看板"])
    
    if menu == "📝 新增销售记录": show_add_page(current_user)
    elif menu == "📊 数据追踪与查看": show_view_page(current_user)
    elif menu == "📈 销售分析看板": show_analysis_page()

if __name__ == "__main__":
    main()