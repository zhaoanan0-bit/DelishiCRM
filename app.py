import streamlit as st
import pandas as pd
import sqlite3
import datetime

# --- 1. 初始化配置 ---
st.set_page_config(page_title="CRM 官方旗舰版", layout="wide")

# 账号密码配置
USER_CREDENTIALS = {
    "超级管理员": "admin123",
    "范秋菊": "fqj888",
    "李秋芳": "lqf888",
    "周梦珂": "zmk888",
    "赵小安": "zxa888"
}

# 选项列表
SHOPS = ["拼多多运动店", "拼多多旗舰店", "天猫旗舰店", "天猫德丽士旗舰店", "淘宝店", "抖店"]
STATUS_LIST = ["初次接触", "已寄样", "报价中", "已签约", "施工中", "已完结", "已流失"]
SITES = ["篮球馆", "羽毛球馆", "乒乓球馆", "健身房", "其他"]

# --- 2. 核心报错防护函数 ---
def safe_f(val):
    """【修复核心】解决平方数、金额等乱码导致的崩溃"""
    if pd.isna(val) or val == "" or val is None or str(val).lower() == 'nan': return 0.0
    try: return float(str(val).replace('¥', '').replace(',', '').strip())
    except: return 0.0

# --- 3. 数据库逻辑 ---
def init_db():
    conn = sqlite3.connect('crm_complete.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT, sales_rep TEXT, customer_name TEXT, phone TEXT, source TEXT, 
        shop_name TEXT, unit_price REAL, area REAL, site_type TEXT, status TEXT, 
        is_construction TEXT, construction_fee REAL, material_fee REAL, 
        shipping_fee REAL, sample_no TEXT, total_amount REAL, 
        follow_up_history TEXT, next_follow_up_date TEXT
    )''')
    conn.commit()
    conn.close()

# --- 4. 登录验证界面 ---
def check_login():
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False
    if not st.session_state["authenticated"]:
        st.title("🔐 CRM 系统安全登录")
        user = st.selectbox("选择登录人", list(USER_CREDENTIALS.keys()))
        pwd = st.text_input("输入密码", type="password")
        if st.button("登录系统"):
            if pwd == USER_CREDENTIALS[user]:
                st.session_state["authenticated"] = True
                st.session_state["username"] = user
                st.rerun()
            else:
                st.error("❌ 密码错误")
        return False
    return True

# --- 5. 页面：新增销售记录 ---
def page_add():
    user = st.session_state["username"]
    st.header(f"📝 录入新销售记录 (当前用户: {user})")
    with st.form("add_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        date_in = c1.date_input("录入日期", datetime.date.today())
        cust_name = c1.text_input("客户名称 (必填)")
        phone = c1.text_input("联系电话")
        
        shop = c2.selectbox("店铺名称", SHOPS)
        site = c2.selectbox("应用场地", SITES)
        source = c2.selectbox("客户来源", ["自然进店", "转介绍", "线下渠道"])
        
        price = c3.number_input("单价(元/㎡)", min_value=0.0)
        area = c3.number_input("平方数(㎡)", min_value=0.0)
        status = c3.selectbox("跟踪进度", STATUS_LIST)

        st.markdown("---")
        c4, c5, c6 = st.columns(3)
        sample_no = c4.text_input("寄样单号")
        is_cons = c4.selectbox("是否施工", ["否", "是"])
        cons_fee = c5.number_input("施工费(元)", min_value=0.0)
        mat_fee = c5.number_input("辅料费(元)", min_value=0.0)
        ship_fee = c6.number_input("运费(元)", min_value=0.0)
        next_date = c6.date_input("计划下次跟进", datetime.date.today() + datetime.timedelta(days=3))
        
        history = st.text_area("沟通记录")
        
        if st.form_submit_button("提交录入"):
            if not cust_name:
                st.error("请填写客户名称")
            else:
                # 预估总金额计算
                total = (price * area) + cons_fee + mat_fee + ship_fee
                conn = sqlite3.connect('crm_complete.db')
                c = conn.cursor()
                c.execute("""INSERT INTO sales (date, sales_rep, customer_name, phone, source, shop_name, 
                             unit_price, area, site_type, status, is_construction, construction_fee, 
                             material_fee, shipping_fee, sample_no, total_amount, follow_up_history, next_follow_up_date) 
                             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", 
                          (str(date_in), user, cust_name, phone, source, shop, price, area, site, status, is_cons, 
                           cons_fee, mat_fee, ship_fee, sample_no, total, history, str(next_date)))
                conn.commit()
                st.success(f"录入成功！预估金额: ¥{total:,.2f}")

# --- 6. 页面：数据追踪与查看 ---
def page_view():
    st.header("📊 数据追踪与跟进")
    conn = sqlite3.connect('crm_complete.db')
    df = pd.read_sql_query("SELECT * FROM sales", conn)
    conn.close()

    if df.empty:
        st.info("暂无记录"); return

    # 逾期逻辑处理
    df['next_dt'] = pd.to_datetime(df['next_follow_up_date'], errors='coerce').dt.date
    overdue = df[df['next_dt'] < datetime.date.today()]
    if not overdue.empty:
        st.warning(f"🔔 有 {len(overdue)} 条逾期未跟进任务！")

    # 权限展示逻辑
    if st.session_state["username"] == "超级管理员":
        rep_filter = st.sidebar.selectbox("筛选销售", ["全部"] + list(USER_CREDENTIALS.keys())[1:])
        display_df = df if rep_filter == "全部" else df[df['sales_rep'] == rep_filter]
    else:
        display_df = df[df['sales_rep'] == st.session_state["username"]]

    st.dataframe(display_df.drop(columns=['next_dt']), use_container_width=True)

    # 管理员编辑区 (修复 Missing Submit Button 和 乱码报错)
    if st.session_state["username"] == "超级管理员":
        st.markdown("---")
        with st.expander("🛠️ 管理员编辑/修改面板"):
            edit_id = st.number_input("输入要修改的 ID", min_value=1, step=1)
            row = df[df['id'] == edit_id]
            if not row.empty:
                rec = row.iloc[0]
                with st.form("edit_form"):
                    col1, col2, col3 = st.columns(3)
                    new_price = col1.number_input("单价", value=safe_f(rec['unit_price']))
                    new_area = col2.number_input("平方数", value=safe_f(rec['area']))
                    new_status = col3.selectbox("进度", STATUS_LIST, index=STATUS_LIST.index(rec['status']) if rec['status'] in STATUS_LIST else 0)
                    
                    if st.form_submit_button("保存修改"):
                        new_total = (new_price * new_area) + safe_f(rec['construction_fee']) + safe_f(rec['material_fee']) + safe_f(rec['shipping_fee'])
                        conn = sqlite3.connect('crm_complete.db')
                        c = conn.cursor()
                        c.execute("UPDATE sales SET unit_price=?, area=?, status=?, total_amount=? WHERE id=?", (new_price, new_area, new_status, new_total, edit_id))
                        conn.commit()
                        st.success("修改成功！")
                        st.rerun()

# --- 7. 页面：分析看板 (全量补齐) ---
def page_analysis():
    st.header("📈 销售分析看板 (全量版)")
    conn = sqlite3.connect('crm_complete.db')
    df = pd.read_sql_query("SELECT * FROM sales", conn)
    conn.close()
    if df.empty: return

    # 对接人业绩统计
    st.subheader("1. 对接人业绩统计")
    rep_stats = df.groupby('sales_rep').agg({'id':'count', 'total_amount':'sum'}).reset_index()
    rep_stats.columns = ['对接人', '跟进项目数', '预估总金额']
    st.table(rep_stats)

    # 店铺转化统计
    st.subheader("2. 店铺渠道统计")
    shop_stats = df.groupby('shop_name').size().reset_index(name='项目数量')
    st.bar_chart(shop_stats.set_index('shop_name'))

# --- 8. 主导航 ---
def main():
    init_db()
    if check_login():
        st.sidebar.title(f"👤 {st.session_state['username']}")
        if st.sidebar.button("登出"):
            st.session_state["authenticated"] = False
            st.rerun()
        
        menu = st.sidebar.radio("功能菜单", ["新增记录", "数据追踪", "分析看板"])
        if menu == "新增记录": page_add()
        elif menu == "数据追踪": page_view()
        elif menu == "分析看板": page_analysis()

if __name__ == "__main__":
    main()