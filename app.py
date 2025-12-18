import streamlit as st
import pandas as pd
import sqlite3
import datetime
import plotly.express as px
import io
import os

# --- 1. 核心配置与样式 ---
st.set_page_config(page_title="CRM全能版", layout="wide")
DB_FILE = 'crm_data.db'

# 定义选项（确保与您的业务一致）
SITE_OPTIONS = ["篮球馆", "羽毛球馆", "乒乓球馆", "健身房", "学校体育馆", "其他"]
SHOP_OPTIONS = ["天猫旗舰店", "拼多多运动店铺", "拼多多旗舰店", "天猫德丽士旗舰店", "淘宝店铺", "抖音店铺", "线下渠道/其他"]
STATUS_OPTIONS = ["初次接触", "已寄样", "报价中", "合同流程", "已签约", "施工中", "已完结/已收款", "流失/搁置", "样品测试"]
INTENT_OPTIONS = ["高", "中", "低", "已成交", "流失"]

# --- 2. 核心修复工具函数 ---

def get_safe_float(value):
    """【解决无法修改的核心】强制转数字，防止编辑框崩溃"""
    if value is None or value == "" or str(value).lower() == "nan":
        return 0.0
    try:
        return float(str(value).replace('¥', '').replace(',', '').strip())
    except:
        return 0.0

def get_safe_date(value):
    """【解决乱码的核心】强制转日期，防止提醒功能报错"""
    if pd.isna(value) or value == "None" or value == "":
        return None
    try:
        return pd.to_datetime(value).date()
    except:
        return None

# --- 3. 数据库逻辑 ---

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # 销售主表
    c.execute('''CREATE TABLE IF NOT EXISTS sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT, sales_rep TEXT, customer_name TEXT, phone TEXT, source TEXT, 
        shop_name TEXT, unit_price REAL, area REAL, site_type TEXT, status TEXT, 
        is_construction TEXT, construction_fee REAL, material_fee REAL, 
        shipping_fee REAL, purchase_intent TEXT, total_amount REAL, 
        follow_up_history TEXT, sample_no TEXT, order_no TEXT, 
        last_follow_up_date TEXT, next_follow_up_date TEXT
    )''')
    conn.commit()
    conn.close()

def get_data():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM sales", conn)
    conn.close()
    return df

# --- 4. 页面：录入新客户 ---

def page_add():
    st.subheader("📝 录入新客户记录")
    with st.form("add_form"):
        c1, c2, c3 = st.columns(3)
        date_in = c1.date_input("录入日期", datetime.date.today())
        cust_name = c1.text_input("客户名称 (必填)")
        shop = c2.selectbox("店铺名称", SHOP_OPTIONS)
        site = c2.selectbox("应用场地", SITE_OPTIONS)
        price = c3.number_input("单价(元/㎡)", min_value=0.0)
        area = c3.number_input("平方数(㎡)", min_value=0.0)
        
        status = c1.selectbox("跟踪进度", STATUS_OPTIONS)
        intent = c2.selectbox("购买意向", INTENT_OPTIONS)
        next_date = c3.date_input("计划下次跟进", datetime.date.today() + datetime.timedelta(days=3))
        
        remark = st.text_area("首次沟通记录")
        submit = st.form_submit_button("提交录入")
        
        if submit and cust_name:
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            total = price * area
            c.execute("""INSERT INTO sales (date, customer_name, shop_name, unit_price, area, site_type, status, purchase_intent, next_follow_up_date, follow_up_history, total_amount) 
                         VALUES (?,?,?,?,?,?,?,?,?,?,?)""", 
                      (str(date_in), cust_name, shop, price, area, site, status, intent, str(next_date), f"首次录入: {remark}", total))
            conn.commit()
            st.success("成功录入！")

# --- 5. 页面：数据追踪 (修复乱码与编辑) ---

def page_view():
    st.subheader("📊 客户列表与追踪")
    df = get_data()
    if df.empty:
        st.info("暂无数据"); return

    # --- 逾期提醒 (修复日期对比报错) ---
    today = datetime.date.today()
    df['next_dt'] = df['next_follow_up_date'].apply(get_safe_date)
    overdue = df[df['next_dt'] < today]
    if not overdue.empty:
        st.error(f"⚠️ 发现 {len(overdue)} 条逾期未跟进记录！")

    # 数据展示
    st.dataframe(df, use_container_width=True)

    # --- 管理员编辑区 (修复无法修改的问题) ---
    st.markdown("---")
    st.subheader("🛠️ 管理员编辑/修改信息")
    edit_id = st.number_input("输入要修改的 ID", min_value=1, step=1)
    
    # 获取该行数据
    row = df[df['id'] == edit_id]
    if not row.empty:
        record = row.iloc[0].to_dict()
        with st.form("edit_form_final"):
            st.write(f"正在修改 ID: {edit_id} - {record['customer_name']}")
            c1, c2 = st.columns(2)
            
            # 使用 get_safe_float 解决无法修改的问题
            new_price = c1.number_input("单价(元/㎡)", value=get_safe_float(record.get('unit_price')))
            new_area = c2.number_input("平方数(㎡)", value=get_safe_float(record.get('area')))
            
            new_status = c1.selectbox("跟踪进度", STATUS_OPTIONS, index=STATUS_OPTIONS.index(record['status']) if record['status'] in STATUS_OPTIONS else 0)
            new_intent = c2.selectbox("购买意向", INTENT_OPTIONS, index=INTENT_OPTIONS.index(record['purchase_intent']) if record['purchase_intent'] in INTENT_OPTIONS else 0)
            
            # 必须有这个按钮
            if st.form_submit_button("保存修改内容"):
                conn = sqlite3.connect(DB_FILE)
                c = conn.cursor()
                new_total = new_price * new_area
                c.execute("UPDATE sales SET unit_price=?, area=?, status=?, purchase_intent=?, total_amount=? WHERE id=?", 
                          (new_price, new_area, new_status, new_intent, new_total, edit_id))
                conn.commit()
                st.success("修改成功！")
                st.rerun()

# --- 6. 页面：分析看板 ---

def page_analysis():
    st.subheader("📈 销售分析看板")
    df = get_data()
    if df.empty: return

    # 简单统计
    c1, c2, c3 = st.columns(3)
    c1.metric("总客户数", len(df))
    c2.metric("已签约数", len(df[df['status']=='已签约']))
    c3.metric("预估总金额", f"¥{df['total_amount'].sum():,.2f}")

    # 仿 Excel 截图的店铺分析
    st.markdown("#### 店铺渠道转化统计")
    shop_stats = df.groupby('shop_name').agg({'id':'count', 'total_amount':'sum'}).reset_index()
    st.table(shop_stats)

# --- 7. 主程序 ---

def main():
    init_db()
    st.sidebar.title("CRM管理系统")
    menu = st.sidebar.radio("功能导航", ["新增销售记录", "数据追踪与查看", "销售分析看板"])
    
    if menu == "新增销售记录": page_add()
    elif menu == "数据追踪与查看": page_view()
    elif menu == "销售分析看板": page_analysis()

if __name__ == "__main__":
    main()