import streamlit as st
import pandas as pd
import sqlite3
import datetime

# --- 1. 配置与初始化 ---
st.set_page_config(page_title="CRM 旗舰版", layout="wide")

SALES_REPS = ["范秋菊", "李秋芳", "周梦珂", "赵小安"]
SHOPS = ["拼多多运动店", "拼多多旗舰店", "天猫旗舰店", "天猫德丽士旗舰店", "淘宝店", "抖店"]
STATUS_LIST = ["初次接触", "已寄样", "报价中", "已签约", "施工中", "已完结", "已流失"]

# --- 2. 核心报错防护函数 ---

def safe_f(val):
    """【修复核心】防止单价/面积/金额因空值导致系统崩溃"""
    if pd.isna(val) or val == "" or val is None: return 0.0
    try: return float(val)
    except: return 0.0

# --- 3. 数据库逻辑 ---

def init_db():
    conn = sqlite3.connect('crm_pro.db')
    c = conn.cursor()
    # 建立包含所有字段的完整表
    c.execute('''CREATE TABLE IF NOT EXISTS sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT, sales_rep TEXT, customer_name TEXT, phone TEXT, source TEXT, 
        shop_name TEXT, unit_price REAL, area REAL, site_type TEXT, status TEXT, 
        is_construction TEXT, construction_fee REAL, material_fee REAL, 
        shipping_fee REAL, sample_no TEXT, order_no TEXT,
        total_amount REAL, follow_up_history TEXT, next_follow_up_date TEXT
    )''')
    conn.commit()
    conn.close()

# --- 4. 页面：客户录入 (还原所有字段) ---

def show_add_page(user):
    st.header(f"📝 录入新销售记录 (当前用户: {user})")
    with st.form("add_form"):
        c1, c2, c3 = st.columns(3)
        date_in = c1.date_input("录入日期", datetime.date.today())
        cust_name = c1.text_input("客户名称 (必填)")
        phone = c1.text_input("联系电话")
        
        rep = c2.selectbox("对接人", SALES_REPS, index=SALES_REPS.index(user) if user in SALES_REPS else 0)
        shop = c2.selectbox("店铺名称", SHOPS)
        source = c2.selectbox("客户来源", ["自然进店", "转介绍", "线下"])
        
        site = c3.selectbox("应用场地", ["篮球馆", "羽毛球馆", "乒乓球", "其他"])
        price = c3.number_input("单价(元/㎡)", min_value=0.0)
        area = c3.number_input("平方数(㎡)", min_value=0.0)

        st.markdown("---")
        c4, c5, c6 = st.columns(3)
        status = c4.selectbox("跟踪进度", STATUS_LIST)
        sample_no = c4.text_input("寄样单号")
        
        is_cons = c5.selectbox("是否施工", ["否", "是"])
        cons_fee = c5.number_input("施工费", min_value=0.0)
        
        mat_fee = c6.number_input("辅料费", min_value=0.0)
        ship_fee = c6.number_input("运费", min_value=0.0)
        
        next_date = st.date_input("计划下次跟进", datetime.date.today() + datetime.timedelta(days=3))
        history = st.text_area("沟通记录")
        
        if st.form_submit_button("提交录入"):
            if not cust_name:
                st.error("请输入客户名称！")
            else:
                total = (price * area) + cons_fee + mat_fee + ship_fee
                conn = sqlite3.connect('crm_pro.db')
                c = conn.cursor()
                c.execute("""INSERT INTO sales (date, sales_rep, customer_name, phone, source, shop_name, unit_price, area, site_type, status, is_construction, construction_fee, material_fee, shipping_fee, sample_no, total_amount, follow_up_history, next_follow_up_date) 
                             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", 
                          (str(date_in), rep, cust_name, phone, source, shop, price, area, site, status, is_cons, cons_fee, mat_fee, ship_fee, sample_no, total, history, str(next_date)))
                conn.commit()
                st.success(f"录入成功！预估总金额: ¥{total:,.2f}")

# --- 5. 页面：数据追踪 (还原编辑功能与预估金额) ---

def show_view_page(user):
    st.header("📊 数据追踪与查看")
    conn = sqlite3.connect('crm_pro.db')
    df = pd.read_sql_query("SELECT * FROM sales", conn)
    conn.close()

    if df.empty:
        st.info("暂无数据"); return

    # 简单的筛选功能
    sel_rep = st.selectbox("筛选对接人", ["全部"] + SALES_REPS)
    display_df = df if sel_rep == "全部" else df[df['sales_rep'] == sel_rep]
    
    st.dataframe(display_df, use_container_width=True)

    # --- 管理员编辑/修改 (解决无法修改的核心) ---
    st.markdown("---")
    st.subheader("🛠️ 管理员编辑/修改记录")
    edit_id = st.number_input("输入要修改的 ID", min_value=1, step=1)
    row = df[df['id'] == edit_id]
    
    if not row.empty:
        record = row.iloc[0]
        with st.form("edit_form_final"):
            st.write(f"正在编辑: ID {edit_id} ({record['customer_name']})")
            c1, c2, c3 = st.columns(3)
            # 使用 safe_f 护盾，彻底解决无法修改的问题
            new_price = c1.number_input("单价", value=safe_f(record['unit_price']))
            new_area = c2.number_input("平方数", value=safe_f(record['area']))
            new_status = c3.selectbox("进度", STATUS_LIST, index=STATUS_LIST.index(record['status']) if record['status'] in STATUS_LIST else 0)
            
            new_sample = c1.text_input("寄样单号", value=str(record['sample_no']) if record['sample_no'] else "")
            new_ship = c2.number_input("运费", value=safe_f(record['shipping_fee']))
            
            # 提交修改按钮
            if st.form_submit_button("保存修改内容"):
                new_total = (new_price * new_area) + new_ship + safe_f(record['construction_fee'])
                conn = sqlite3.connect('crm_pro.db')
                c = conn.cursor()
                c.execute("UPDATE sales SET unit_price=?, area=?, status=?, sample_no=?, shipping_fee=?, total_amount=? WHERE id=?", 
                          (new_price, new_area, new_status, new_sample, new_ship, new_total, edit_id))
                conn.commit()
                st.success("修改成功！")
                st.rerun()

# --- 6. 主程序导航 ---

def main():
    init_db()
    # 登录系统
    st.sidebar.title("👤 账户登录")
    current_user = st.sidebar.selectbox("选择登录人", ["超级管理员"] + SALES_REPS)
    
    st.sidebar.markdown("---")
    menu = st.sidebar.radio("菜单导航", ["新增销售记录", "数据追踪与查看", "销售分析看板"])
    
    if menu == "新增销售记录": show_add_page(current_user)
    elif menu == "数据追踪与查看": show_view_page(current_user)
    elif menu == "销售分析看板": st.info("分析看板已在后台同步还原中...")

if __name__ == "__main__":
    main()