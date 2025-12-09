import streamlit as st
import pandas as pd
import sqlite3
import datetime
import plotly.express as px

# --- 配置与数据初始化 ---
DB_FILE = 'crm_data.db'

# 1. 用户账号配置 (已包含您的定制账号 zhaoxiaoan)
USERS = {
    'admin': {'password': 'admin123', 'role': 'admin'},      # 默认管理员
    'sales1': {'password': '123', 'role': 'user'},           # 默认普通用户
    'sales2': {'password': '123', 'role': 'user'},
    'zhaoxiaoan': {'password': 'zhaoxiaoan123', 'role': 'admin'}, # 您的自定义账号
}

# 2. 下拉选项配置 (已更新应用场地和店铺名字)
SITE_OPTIONS = [
    # 1. 专业体育场馆
    "篮球馆（FIBA认证场地）", "排球馆", "羽毛球馆", "乒乓球馆", 
    "室内网球场", "手球馆", "室内足球/五人制足球场",
    # 2. 学校及教育机构
    "学校体育馆", "幼儿园室内活动室", "小学/中学/大学多功能运动场", "室内操场/风雨操场",
    # 3. 健身房与训练中心
    "综合健身房", "瑜伽馆、舞蹈室", "搏击/武术训练馆", "跨界训练（CrossFit）场地", "体能康复训练中心",
    # 4. 商业及社区场馆
    "社区体育中心", "企事业单位职工活动中心", "商业连锁健身房", "青少年培训机构",
    # 5. 其他特殊场所
    "轮滑场", "壁球馆", "室内滑冰训练辅助区", "部队、公安、消防训练馆", "医院康复科运动治疗室", 
    "老年活动中心", "其他/未分类"
]

SHOP_OPTIONS = [
    "天猫旗舰店", "拼多多运动店铺", "拼多多旗舰店", "淘宝店铺", "抖音店铺", "线下渠道/其他"
]

STATUS_OPTIONS = ["初次接触", "已寄样", "报价中", "合同流程", "施工中", "已完结/已收款"]
INTENT_OPTIONS = ["高", "中", "低", "已成交", "流失"]


# --- 数据库函数 ---
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # 创建表，包含所有字段
    c.execute('''CREATE TABLE IF NOT EXISTS sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        sales_rep TEXT,
        customer_name TEXT,
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
        remarks TEXT,
        sample_no TEXT,
        order_no TEXT
    )''')
    conn.commit()
    conn.close()

def add_data(data):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''INSERT INTO sales (
        date, sales_rep, customer_name, shop_name, unit_price, area, 
        site_type, status, is_construction, construction_fee, material_fee, 
        purchase_intent, total_amount, remarks, sample_no, order_no
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', data)
    conn.commit()
    conn.close()

def get_data():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM sales", conn)
    conn.close()
    return df

def delete_data(record_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM sales WHERE id=?", (record_id,))
    conn.commit()
    conn.close()

# --- 登录逻辑 ---
def check_password():
    """验证用户登录并设置 session 状态"""
    def password_entered():
        if st.session_state["username"] in USERS and \
           st.session_state["password"] == USERS[st.session_state["username"]]['password']:
            st.session_state["password_correct"] = True
            st.session_state["role"] = USERS[st.session_state["username"]]['role']
            st.session_state["user_now"] = st.session_state["username"]
            del st.session_state["password"]  # 不存储密码
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # 初始状态，显示登录界面
        st.header("🏢 CRM 系统登录")
        st.text_input("用户名", key="username")
        st.text_input("密码", type="password", key="password")
        st.button("登录", on_click=password_entered)
        return False
    elif not st.session_state["password_correct"]:
        # 密码错误，显示登录界面和错误信息
        st.header("🏢 CRM 系统登录")
        st.text_input("用户名", key="username")
        st.text_input("密码", type="password", key="password")
        st.button("登录", on_click=password_entered)
        st.error("用户不存在或密码错误")
        return False
    else:
        # 登录成功
        return True

# --- 主程序 ---
def main():
    st.set_page_config(page_title="简易销售CRM系统", layout="wide")
    init_db()

    if check_password():
        user_role = st.session_state["role"]
        current_user = st.session_state["user_now"]
        
        # 侧边栏导航
        st.sidebar.title(f"🎉 欢迎, {current_user}")
        if user_role == 'admin':
             st.sidebar.caption("当前权限：管理员 (可删除数据)")
        else:
             st.sidebar.caption("当前权限：普通销售 (仅录入/查看)")
             
        st.sidebar.markdown("---")
        menu = ["📝 新增销售记录", "📊 数据追踪与查看", "📈 销售分析看板"]
        choice = st.sidebar.radio("导航菜单", menu)
        st.sidebar.markdown("---")

        # 1. 新增记录页面
        if choice == "📝 新增销售记录":
            st.subheader("客户信息录入")
            with st.form("entry_form", clear_on_submit=True):
                col1, col2, col3 = st.columns(3)
                with col1:
                    date_val = st.date_input("日期", datetime.date.today())
                    customer_name = st.text_input("客户名称")
                    # 使用更新后的店铺选项
                    shop_name = st.selectbox("店铺名字", SHOP_OPTIONS)
                    # 使用更新后的应用场地选项
                    site_type = st.selectbox("应用场地", SITE_OPTIONS)
                
                with col2:
                    unit_price = st.number_input("单价 (元/㎡)", min_value=0.0, step=0.1, format="%.2f")
                    area = st.number_input("平方数 (㎡)", min_value=0.0, step=0.1, format="%.2f")
                    is_const = st.selectbox("是否施工", ["否", "是"])
                    const_fee = st.number_input("施工费 (元)", min_value=0.0, step=100.0)
                
                with col3:
                    mat_fee = st.number_input("辅料费用 (元)", min_value=0.0, step=50.0)
                    purchase_intent = st.selectbox("购买意向", INTENT_OPTIONS)
                    status = st.selectbox("跟踪进度", STATUS_OPTIONS)
                    # 对接人自动锁定为当前登录用户
                    sales_rep_input = st.text_input("对接人", value=current_user, disabled=True)

                # 预估总金额计算
                calc_total_preview = (unit_price * area) + const_fee + mat_fee
                st.markdown(f"**💰 预估总金额 (元)：** `{calc_total_preview:,.2f}`")

                st.markdown("---")
                col4, col5 = st.columns(2)
                with col4:
                    sample_no = st.text_input("寄样单号")
                    order_no = st.text_input("订单号")
                with col5:
                    remarks = st.text_area("备注信息")

                submitted = st.form_submit_button("✅ 提交录入")

                if submitted:
                    if customer_name == "":
                        st.warning("⚠️ 请填写客户名称！")
                    else:
                        # 提交时重新计算并写入数据库
                        calc_total = (unit_price * area) + const_fee + mat_fee
                        data_tuple = (
                            date_val, current_user, customer_name, shop_name, unit_price, area,
                            site_type, status, is_const, const_fee, mat_fee,
                            purchase_intent, calc_total, remarks, sample_no, order_no
                        )
                        add_data(data_tuple)
                        st.success(f"🎉 客户 **{customer_name}** 录入成功！总金额: **{calc_total:,.2f}** 元")

        # 2. 数据查看页面
        elif choice == "📊 数据追踪与查看":
            st.subheader("客户追踪列表")
            df = get_data()
            
            # 搜索/过滤功能
            st.markdown("---")
            col_filter, col_search = st.columns([1, 2])
            
            with col_filter:
                filter_rep = st.selectbox("按对接人筛选", ['全部'] + df['sales_rep'].unique().tolist())
            
            with col_search:
                search_term = st.text_input("🔍 搜索客户名称、店铺或订单号")

            df_filtered = df.copy()
            if filter_rep != '全部':
                df_filtered = df_filtered[df_filtered['sales_rep'] == filter_rep]
            
            if search_term:
                df_filtered = df_filtered[
                    df_filtered['customer_name'].str.contains(search_term, case=False) |
                    df_filtered['shop_name'].str.contains(search_term, case=False) |
                    df_filtered['order_no'].astype(str).str.contains(search_term, case=False)
                ]

            # 展示数据
            st.dataframe(df_filtered, use_container_width=True, hide_index=True)

            # 管理员特权：删除数据
            if user_role == 'admin':
                st.markdown("### ⚠️ 管理员操作区")
                col_del, _ = st.columns([1, 3])
                with col_del:
                    del_id = st.number_input("输入要删除的记录 ID", min_value=0, step=1, help="请查看表格第一列的 ID")
                    if st.button("🔴 永久删除记录"):
                        delete_data(del_id)
                        st.success(f"ID {del_id} 记录已删除。")
                        st.rerun()
            else:
                st.info("💡 普通用户仅可查看和新增，如需修改/删除请联系管理员。")

        # 3. 分析页面
        elif choice == "📈 销售分析看板":
            st.subheader("销售数据分析")
            df = get_data()
            if not df.empty:
                # 数据类型转换，确保能计算
                df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce').fillna(0)
                
                # 关键指标卡片
                total_sales = df['total_amount'].sum()
                total_orders = len(df)
                avg_order = df['total_amount'].mean() 

                c1, c2, c3 = st.columns(3)
                c1.metric("💰 累计销售总额", f"¥{total_sales:,.2f}")
                c2.metric("📦 总记录数", f"{total_orders} 单")
                c3.metric("🏷️ 平均客单价", f"¥{avg_order:,.2f}")

                st.markdown("---")
                
                # 图表区
                col_chart1, col_chart2 = st.columns(2)
                
                with col_chart1:
                    st.markdown("#### 各对接人业绩对比")
                    rep_sales = df.groupby('sales_rep')['total_amount'].sum().sort_values(ascending=False).reset_index()
                    fig_rep = px.bar(rep_sales, x='sales_rep', y='total_amount', color='sales_rep', title="对接人销售额（元）")
                    st.plotly_chart(fig_rep, use_container_width=True)

                with col_chart2:
                    st.markdown("#### 客户意向分布")
                    intent_counts = df['purchase_intent'].value_counts().reset_index()
                    intent_counts.columns = ['purchase_intent', 'count']
                    fig_intent = px.pie(intent_counts, values='count', names='purchase_intent', title="购买意向占比", hole=0.3)
                    st.plotly_chart(fig_intent, use_container_width=True)
            else:
                st.warning("暂无数据，请先录入销售信息。")

if __name__ == '__main__':
    main()