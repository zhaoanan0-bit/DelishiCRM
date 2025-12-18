import streamlit as st
import pandas as pd
import sqlite3
import datetime

# --- 1. 初始化配置 ---
st.set_page_config(page_title="CRM 官方旗舰版", layout="wide")

# 账号密码配置
USER_CREDENTIALS = {
    "超级管理员": "admin123", "范秋菊": "fqj888", "李秋芳": "lqf888", "周梦珂": "zmk888", "赵小安": "zxa888"
}

# 业务选项
SHOPS = ["拼多多运动店", "拼多多旗舰店", "天猫旗舰店", "天猫德丽士旗舰店", "淘宝店", "抖店", "线下渠道/其他"]
STATUS_LIST = ["初次接触", "方案报价", "已寄样", "价格谈判", "已签约", "施工中", "已完结", "已流失"]

# --- 2. 数据库与安全函数 ---
def init_db():
    conn = sqlite3.connect('crm_v4_final.db')
    c = conn.cursor()
    # 建立全中文命名的数据库表
    c.execute('''CREATE TABLE IF NOT EXISTS sales_data (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        录入日期 TEXT, 对接人 TEXT, 客户名称 TEXT, 联系电话 TEXT, 客户来源 TEXT, 
        店铺名称 TEXT, 单价 REAL, 平方数 REAL, 应用场地 TEXT, 跟踪进度 TEXT, 
        是否施工 TEXT, 施工费 REAL, 辅料费 REAL, 运费 REAL, 寄样单号 TEXT, 
        预估总金额 REAL, 沟通记录 TEXT, 下次跟进日期 TEXT
    )''')
    conn.commit()
    conn.close()

def safe_float(val):
    """防止修改信息时因空值乱码导致报错"""
    if pd.isna(val) or val == "" or val is None: return 0.0
    try: return float(str(val).replace('¥', '').replace(',', '').strip())
    except: return 0.0

# --- 3. 登录逻辑 ---
if "auth" not in st.session_state:
    st.session_state["auth"] = False

if not st.session_state["auth"]:
    st.title("🔐 欢迎登录 CRM 管理系统")
    user = st.selectbox("选择账号", list(USER_CREDENTIALS.keys()))
    pwd = st.text_input("输入密码", type="password")
    if st.button("立即登录"):
        if pwd == USER_CREDENTIALS[user]:
            st.session_state["auth"] = True
            st.session_state["user"] = user
            st.rerun()
        else: st.error("密码错误")
    st.stop()

# --- 4. 侧边栏导航 ---
st.sidebar.title(f"👤 {st.session_state['user']}")
menu = st.sidebar.radio("功能菜单", ["📝 新增销售记录", "📊 数据追踪与跟进", "📈 销售分析看板"])
if st.sidebar.button("退出系统"):
    st.session_state["auth"] = False
    st.rerun()

init_db()

# --- 5. 功能模块 ---

if menu == "📝 新增销售记录":
    st.header(f"📝 录入新销售记录 (登录人: {st.session_state['user']})")
    with st.form("add_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        date_in = c1.date_input("录入日期", datetime.date.today())
        cust_name = c1.text_input("客户名称 (必填)")
        phone = c1.text_input("联系电话")
        
        rep = c2.selectbox("对接人", list(USER_CREDENTIALS.keys())[1:], index=list(USER_CREDENTIALS.keys()).index(st.session_state['user'])-1 if st.session_state['user'] != "超级管理员" else 0)
        shop = c2.selectbox("店铺名称", SHOPS)
        site = c2.selectbox("应用场地", ["篮球馆", "羽毛球馆", "乒乓球馆", "风雨操场", "其他"])
        
        price = c3.number_input("单价(元/㎡)", min_value=0.0)
        area = c3.number_input("平方数(㎡)", min_value=0.0)
        source = c3.selectbox("客户来源", ["自然进店", "转介绍", "线下渠道", "平台推广"])

        st.markdown("---")
        c4, c5, c6 = st.columns(3)
        status = c4.selectbox("跟踪进度", STATUS_LIST)
        sample = c4.text_input("寄样单号")
        is_cons = c4.selectbox("是否施工", ["否", "是"])
        
        cons_fee = c5.number_input("施工费(元)", min_value=0.0)
        mat_fee = c5.number_input("辅料费(元)", min_value=0.0)
        ship_fee = c6.number_input("运费(独立计算)", min_value=0.0)
        next_date = c6.date_input("计划下次跟进", datetime.date.today() + datetime.timedelta(days=3))
        
        history = st.text_area("沟通记录/备注")
        
        if st.form_submit_button("确认提交数据"):
            if not cust_name:
                st.error("请务必填写客户名称")
            else:
                # 预估总金额：单价*平方 + 施工 + 辅料（运费在表格中独立体现）
                total = (price * area) + cons_fee + mat_fee
                conn = sqlite3.connect('crm_v4_final.db')
                c = conn.cursor()
                c.execute("""INSERT INTO sales_data (录入日期, 对接人, 客户名称, 联系电话, 客户来源, 店铺名称, 单价, 平方数, 应用场地, 跟踪进度, 是否施工, 施工费, 辅料费, 运费, 寄样单号, 预估总金额, 沟通记录, 下次跟进日期) 
                             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", 
                          (str(date_in), rep, cust_name, phone, source, shop, price, area, site, status, is_cons, cons_fee, mat_fee, ship_fee, sample, total, history, str(next_date)))
                conn.commit()
                st.success(f"✅ 录入成功！预估金额(含施工辅料): ¥{total:,.2f}，运费单独记录: ¥{ship_fee:,.2f}")

elif menu == "📊 数据追踪与跟进":
    st.header("📊 客户追踪列表")
    conn = sqlite3.connect('crm_v4_final.db')
    df = pd.read_sql_query("SELECT * FROM sales_data", conn)
    conn.close()

    if df.empty:
        st.info("暂无记录")
    else:
        # 权限过滤
        if st.session_state["user"] != "超级管理员":
            df = df[df['对接人'] == st.session_state["user"]]
        
        st.dataframe(df, use_container_width=True, hide_index=True)

        # 管理员编辑区 (修复无法修改、平方数乱码等问题)
        if st.session_state["user"] == "超级管理员":
            st.markdown("---")
            with st.expander("🛠️ 管理员修改/编辑记录"):
                edit_id = st.number_input("输入要修改的项目 ID", min_value=1, step=1)
                row = df[df['ID'] == edit_id]
                if not row.empty:
                    rec = row.iloc[0]
                    with st.form("edit_form"):
                        c1, c2, c3 = st.columns(3)
                        # 核心修复：使用 safe_float 载入数据，防止乱码报错
                        e_price = c1.number_input("单价", value=safe_float(rec['单价']))
                        e_area = c2.number_input("平方数", value=safe_float(rec['平方数']))
                        e_ship = c3.number_input("运费", value=safe_float(rec['运费']))
                        e_status = c1.selectbox("跟踪进度", STATUS_LIST, index=STATUS_LIST.index(rec['跟踪进度']) if rec['跟踪进度'] in STATUS_LIST else 0)
                        
                        if st.form_submit_button("确认保存修改"):
                            new_total = (e_price * e_area) + safe_float(rec['施工费']) + safe_float(rec['辅料费'])
                            conn = sqlite3.connect('crm_v4_final.db')
                            c = conn.cursor()
                            c.execute("UPDATE sales_data SET 单价=?, 平方数=?, 运费=?, 跟踪进度=?, 预估总金额=? WHERE ID=?", 
                                      (e_price, e_area, e_ship, e_status, new_total, edit_id))
                            conn.commit()
                            st.success("信息已更新！")
                            st.rerun()

elif menu == "📈 销售分析看板":
    st.header("📈 销售分析看板")
    conn = sqlite3.connect('crm_v4_final.db')
    df = pd.read_sql_query("SELECT * FROM sales_data", conn)
    conn.close()
    
    if not df.empty:
        # 预计算签约标识
        df['是否签约'] = df['跟踪进度'].apply(lambda x: 1 if x == '已签约' else 0)
        df['签约金额'] = df.apply(lambda x: x['预估总金额'] if x['是否签约'] == 1 else 0, axis=1)

        # 表1：对接人业绩统计
        st.subheader("1. 销售对接人业绩榜")
        rep_stats = df.groupby('对接人').agg(
            跟进项目数=('ID', 'count'),
            签约数=('是否签约', 'sum'),
            签约总金额=('签约金额', 'sum')
        ).reset_index()
        rep_stats['签约率'] = (rep_stats['签约数'] / rep_stats['跟进项目数']).map(lambda x: f"{x:.1%}")
        st.table(rep_stats)

        # 表2：店铺渠道转化统计
        st.subheader("2. 店铺渠道转化统计")
        shop_stats = df.groupby('店铺名称').agg(
            项目数量=('ID', 'count'),
            签约数量=('是否签约', 'sum'),
            签约总金额=('签约金额', 'sum')
        ).reset_index()
        shop_stats['转化率'] = (shop_stats['签约数量'] / shop_stats['项目数量']).map(lambda x: f"{x:.1%}")
        st.dataframe(shop_stats, hide_index=True)