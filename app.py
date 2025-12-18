import streamlit as st
import pandas as pd
import sqlite3
import datetime

# --- 1. 初始化页面配置 ---
st.set_page_config(page_title="CRM 官方旗舰版", layout="wide")

# 账号密码配置
USER_CREDENTIALS = {
    "超级管理员": "admin123", "范秋菊": "fqj888", "李秋芳": "lqf888", "周梦珂": "zmk888", "赵小安": "zxa888"
}

# 核心选项列表
SHOPS = ["拼多多运动店", "拼多多旗舰店", "天猫旗舰店", "天猫德丽士旗舰店", "淘宝店", "抖店", "线下渠道"]
STATUS_LIST = ["初次接触", "方案报价", "已寄样", "样品测试", "价格谈判", "已签约", "施工中", "已完结", "已流失"]
SITES = ["篮球馆", "羽毛球馆", "乒乓球馆", "健身房", "其他"]

# --- 2. 数据库与安全转换函数 ---
def init_db():
    conn = sqlite3.connect('crm_ultimate.db')
    c = conn.cursor()
    # 建立全中文命名的数据库表，严格对照
    c.execute('''CREATE TABLE IF NOT EXISTS sales_data (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        录入日期 TEXT, 对接人 TEXT, 客户名称 TEXT, 联系电话 TEXT, 客户来源 TEXT, 
        店铺名称 TEXT, 单价 REAL, 平方数 REAL, 应用场地 TEXT, 跟踪进度 TEXT, 
        是否施工 TEXT, 施工费 REAL, 辅料费 REAL, 运费 REAL, 寄样单号 TEXT, 
        预估总金额 REAL, 沟通记录 TEXT, 下次跟进日期 TEXT
    )''')
    conn.commit()
    conn.close()

def clean_f(val):
    """【彻底修复崩溃】处理数值中的nan、¥和空字符"""
    if pd.isna(val) or val == "" or str(val).lower() == 'nan': return 0.0
    try: return float(str(val).replace('¥', '').replace(',', '').strip())
    except: return 0.0

# --- 3. 安全登录校验 ---
if "auth" not in st.session_state: st.session_state["auth"] = False

if not st.session_state["auth"]:
    st.title("🔐 CRM 系统安全登录")
    u = st.selectbox("选择账号", list(USER_CREDENTIALS.keys()))
    p = st.text_input("输入密码", type="password")
    if st.button("进入系统"):
        if p == USER_CREDENTIALS[u]:
            st.session_state["auth"], st.session_state["user"] = True, u
            st.rerun()
        else: st.error("❌ 密码错误")
    st.stop()

init_db()

# --- 4. 侧边栏 ---
st.sidebar.title(f"👤 {st.session_state['user']}")
menu = st.sidebar.radio("菜单导航", ["📝 新增销售记录", "📊 客户追踪看板", "📈 业绩统计看板"])
if st.sidebar.button("登出系统"):
    st.session_state["auth"] = False
    st.rerun()

# --- 5. 功能模块 ---

if menu == "📝 新增销售记录":
    st.header(f"📝 录入新客户记录 (对接人: {st.session_state['user']})")
    with st.form("add_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        # 第一列：基础信息
        d_in = c1.date_input("录入日期", datetime.date.today())
        c_name = c1.text_input("客户名称 (必填)")
        c_phone = c1.text_input("联系电话")
        
        # 第二列：店铺与应用
        shop = c2.selectbox("店铺名称", SHOPS)
        site = c2.selectbox("应用场地", SITES)
        source = c2.selectbox("客户来源", ["自然进店", "转介绍", "线下渠道"])
        
        # 第三列：价格与面积
        price = c3.number_input("单价(元/㎡)", min_value=0.0)
        area = c3.number_input("平方数(㎡)", min_value=0.0)
        status = c3.selectbox("跟踪进度", STATUS_LIST)

        st.markdown("---")
        c4, c5, c6 = st.columns(3)
        sample = c4.text_input("寄样单号")
        is_cons = c4.selectbox("是否施工", ["否", "是"])
        
        cons_fee = c5.number_input("施工费(元)", min_value=0.0)
        mat_fee = c5.number_input("辅料费(元)", min_value=0.0)
        
        ship_fee = c6.number_input("运费 (独立计算)", min_value=0.0) # 运费独立
        next_dt = c6.date_input("计划下次跟进", datetime.date.today() + datetime.timedelta(days=3))
        
        history = st.text_area("沟通历史/备注")
        
        if st.form_submit_button("✅ 确认录入"):
            if not c_name: st.error("请填写客户名称")
            else:
                # 预估总金额：单价*面积 + 施工 + 辅料 (运费独立)
                total = (price * area) + cons_fee + mat_fee
                conn = sqlite3.connect('crm_ultimate.db')
                c = conn.cursor()
                c.execute("""INSERT INTO sales_data (录入日期, 对接人, 客户名称, 联系电话, 客户来源, 店铺名称, 
                             单价, 平方数, 应用场地, 跟踪进度, 是否施工, 施工费, 辅料费, 运费, 寄样单号, 
                             预估总金额, 沟通记录, 下次跟进日期) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", 
                          (str(d_in), st.session_state['user'], c_name, c_phone, source, shop, price, area, site, status, 
                           is_cons, cons_fee, mat_fee, ship_fee, sample, total, history, str(next_dt)))
                conn.commit()
                st.success(f"成功录入！预估总金额: ¥{total:,.2f}")

elif menu == "📊 客户追踪看板":
    st.header("📊 客户追踪列表")
    conn = sqlite3.connect('crm_ultimate.db')
    df = pd.read_sql_query("SELECT * FROM sales_data", conn)
    conn.close()

    if df.empty: st.info("暂无数据")
    else:
        # 搜索与过滤
        kw = st.text_input("🔍 搜索客户姓名或联系电话")
        if kw: df = df[df['客户名称'].str.contains(kw) | df['联系电话'].str.contains(kw)]
        
        if st.session_state["user"] != "超级管理员":
            df = df[df['对接人'] == st.session_state["user"]]
        
        # 格式化显示预估金额
        df_show = df.copy()
        df_show['预估总金额'] = df_show['预估总金额'].apply(lambda x: f"¥{x:,.2f}")
        st.dataframe(df_show, use_container_width=True, hide_index=True)

        # 管理员编辑区 (修复 Missing Button)
        if st.session_state["user"] == "超级管理员":
            st.markdown("---")
            with st.expander("🛠️ 修改客户进度/信息"):
                e_id = st.number_input("输入要修改的项目 ID", min_value=1, step=1)
                target = df[df['ID'] == e_id]
                if not target.empty:
                    rec = target.iloc[0]
                    with st.form("edit_real_form"):
                        col1, col2 = st.columns(2)
                        new_status = col1.selectbox("修改进度", STATUS_LIST, index=STATUS_LIST.index(rec['跟踪进度']))
                        new_area = col2.number_input("修改平方数", value=clean_f(rec['平方数']))
                        new_note = st.text_area("追加备注", value=rec['沟通记录'])
                        if st.form_submit_button("保存修改"):
                            # 重新计算总额
                            new_total = (clean_f(rec['单价']) * new_area) + clean_f(rec['施工费']) + clean_f(rec['辅料费'])
                            conn = sqlite3.connect('crm_ultimate.db')
                            c = conn.cursor()
                            c.execute("UPDATE sales_data SET 跟踪进度=?, 平方数=?, 沟通记录=?, 预估总金额=? WHERE ID=?", 
                                      (new_status, new_area, new_note, new_total, e_id))
                            conn.commit()
                            st.success("修改成功！")
                            st.rerun()

elif menu == "📈 业绩统计看板":
    st.header("📈 业绩统计与转化看板")
    conn = sqlite3.connect('crm_ultimate.db')
    df = pd.read_sql_query("SELECT * FROM sales_data", conn)
    conn.close()
    
    if not df.empty:
        # 预计算逻辑
        df['已签约'] = df['跟踪进度'].apply(lambda x: 1 if x == '已签约' else 0)
        df['签约金额'] = df.apply(lambda x: x['预估总金额'] if x['已签约'] == 1 else 0, axis=1)

        # 表1：对接人统计
        st.subheader("1. 销售对接人业绩榜")
        rep_tab = df.groupby('对接人').agg(
            跟进项目数=('ID', 'count'),
            已签约数=('已签约', 'sum'),
            签约总金额=('签约金额', 'sum')
        ).reset_index()
        rep_tab['签约率'] = (rep_tab['已签约数'] / rep_tab['跟进项目数']).map(lambda x: f"{x:.1%}")
        rep_tab['平均客单价'] = (rep_tab['签约总金额'] / rep_tab['已签约数']).fillna(0).apply(lambda x: f"¥{x:,.0f}")
        st.table(rep_tab)

        # 表2：店铺统计
        st.subheader("2. 店铺渠道转化统计")
        shop_tab = df.groupby('店铺名称').agg(
            项目数量=('ID', 'count'),
            签约数量=('已签约', 'sum'),
            签约总额=('签约金额', 'sum')
        ).reset_index()
        shop_tab['签约率'] = (shop_tab['签约数量'] / shop_tab['项目数量']).map(lambda x: f"{x:.1%}")
        st.dataframe(shop_tab, use_container_width=True, hide_index=True)