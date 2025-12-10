import streamlit as st
import pandas as pd
import sqlite3
import datetime
import plotly.express as px
import numpy as np
import io

# --- 配置与数据初始化 ---
DB_FILE = 'crm_data.db'
DAYS_FOR_TRANSFER = 20 

# 1. 用户账号配置
USERS = {
    'admin': {'password': 'admin123', 'role': 'admin'},
    'zhaoxiaoan': {'password': 'zhaoxiaoan123', 'role': 'admin'},
    'liqiufang': {'password': '123', 'role': 'user'}, 
    'fanqiuju': {'password': '123', 'role': 'user'},
    'zhoumengke': {'password': '123', 'role': 'user'},
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
SOURCE_OPTIONS = ["自然进店", "抖音/快手推广", "老客户转介绍", "地推/线下活动", "招标/公海", "其他"] # 新增：客户来源

# --- 数据库函数 ---
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        sales_rep TEXT,
        customer_name TEXT,
        phone TEXT,              -- 新增：联系电话（用于查重）
        source TEXT,             -- 新增：客户来源
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
        follow_up_history TEXT,  -- 修改：将备注改为“跟进历史记录”
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

def delete_data(record_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM sales WHERE id=?", (record_id,))
    conn.commit()
    conn.close()

def transfer_sales_rep(record_id, new_rep):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # 自动追加一条转交记录
    log = f"\n[{datetime.date.today()}] 系统自动转交：客户超期，已转交给 {new_rep}"
    c.execute("UPDATE sales SET sales_rep=?, status='转交管理', last_follow_up_date=?, follow_up_history=follow_up_history || ? WHERE id=?", 
              (new_rep, datetime.date.today().isoformat(), log, record_id))
    conn.commit()
    conn.close()

# 新增：追加跟进记录函数
def update_follow_up(record_id, new_log, next_date, new_status, new_intent):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # 追加模式：保留旧记录，增加新记录
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

# 新增：检查客户是否存在
def check_customer_exist(name, phone):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT sales_rep FROM sales WHERE customer_name=? OR (phone!='' AND phone=?)", (name, phone))
    result = c.fetchone()
    conn.close()
    return result[0] if result else None

# --- 登录逻辑 ---
def check_password():
    def password_entered():
        if st.session_state["username"] in USERS and \
           st.session_state["password"] == USERS[st.session_state["username"]]['password']:
            st.session_state["password_correct"] = True
            st.session_state["role"] = USERS[st.session_state["username"]]['role']
            st.session_state["user_now"] = st.session_state["username"]
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
    init_db()

    if check_password():
        user_role = st.session_state["role"]
        current_user = st.session_state["user_now"]
        
        st.sidebar.title(f"👤 {current_user}")
        menu = ["📝 新增销售记录", "📊 数据追踪与查看", "📈 销售分析看板"]
        choice = st.sidebar.radio("菜单", menu)
        
        # --- 侧边栏：数据导出 (新增功能) ---
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 💾 数据备份")
        if st.sidebar.button("下载 Excel 备份"):
            df_export = get_data()
            if not df_export.empty:
                # 转换 Pandas DataFrame 为 Excel 字节流
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_export.to_excel(writer, index=False, sheet_name='Sheet1')
                excel_data = output.getvalue()
                st.sidebar.download_button(label="📥 点击保存文件", data=excel_data, file_name=f'CRM_Backup_{datetime.date.today()}.xlsx', mime='application/vnd.ms-excel')
            else:
                st.sidebar.warning("暂无数据可导出")

        # 1. 新增记录页面
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
                    sales_rep_input = st.text_input("对接人", value=current_user, disabled=True)

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
                    # 初始备注
                    first_remark = st.text_area("首次沟通记录")

                submitted = st.form_submit_button("✅ 提交录入")

                if submitted:
                    if customer_name == "":
                        st.warning("⚠️ 客户名称不能为空")
                    else:
                        # 查重逻辑
                        existing_rep = check_customer_exist(customer_name, phone)
                        if existing_rep:
                            st.error(f"❌ 录入失败！该客户已存在，目前由 **{existing_rep}** 负责。请勿重复录入。")
                        else:
                            calc_total = (unit_price * area) + const_fee + mat_fee
                            # 格式化跟进记录
                            log_entry = f"[{datetime.date.today()} {current_user}]: 首次录入。{first_remark}"
                            
                            data_tuple = (
                                date_val, current_user, customer_name, phone, source, shop_name, unit_price, area,
                                site_type, status, is_const, const_fee, mat_fee,
                                purchase_intent, calc_total, log_entry, sample_no, order_no,
                                str(last_fup), str(next_fup)
                            )
                            add_data(data_tuple)
                            st.success(f"🎉 客户 {customer_name} 录入成功！")

        # 2. 数据查看页面
        elif choice == "📊 数据追踪与查看":
            st.subheader("📋 客户追踪列表")
            df = get_data()
            
            # --- 快速追加跟进记录功能 (新增) ---
            with st.expander("➕ 快速追加跟进记录 (无需进表单修改)"):
                col_up1, col_up2 = st.columns([1, 2])
                with col_up1:
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
                    # 简单验证 ID 是否存在
                    if not df.empty and up_id in df['id'].values:
                         # 验证权限：只能改自己的，或者是管理员
                        record_rep = df[df['id'] == up_id]['sales_rep'].values[0]
                        if user_role == 'admin' or record_rep == current_user:
                            new_log = f"[{datetime.date.today()} {current_user}]: {up_content}"
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
                today = datetime.date.today()
                
                # 计算超期
                df['days_since_fup'] = (pd.to_datetime(today) - df['last_follow_up_date']).dt.days
                
                # 提醒显示
                overdue = df[(df['status'] != '已完结/已收款') & (df['days_since_fup'] > DAYS_FOR_TRANSFER)]
                if user_role == 'admin' and not overdue.empty:
                    st.error(f"⚠️ 管理员注意：有 {len(overdue)} 个客户超 {DAYS_FOR_TRANSFER} 天未跟进！")
                    if st.button("🔥 一键接管所有超期客户"):
                        for pid in overdue['id'].values:
                            transfer_sales_rep(pid, 'admin')
                        st.success("已全部转入管理员名下")
                        st.rerun()

                # 普通用户提醒
                my_reminders = df[
                    (df['next_follow_up_date'].dt.date <= today) & 
                    (df['status'] != '已完结/已收款') &
                    (df['sales_rep'] == current_user)
                ]
                if not my_reminders.empty:
                    st.warning(f"🔔 {current_user}，您今天有 {len(my_reminders)} 个待办跟进！")

                # 表格显示
                col_filter, col_search = st.columns([1, 2])
                with col_filter:
                    filter_rep = st.selectbox("对接人筛选", ['全部'] + df['sales_rep'].unique().tolist())
                with col_search:
                    search_term = st.text_input("🔍 搜客户、电话或店铺")

                df_show = df.copy()
                if filter_rep != '全部':
                    df_show = df_show[df_show['sales_rep'] == filter_rep]
                if search_term:
                    df_show = df_show[
                        df_show['customer_name'].astype(str).str.contains(search_term, case=False) |
                        df_show['phone'].astype(str).str.contains(search_term, case=False) |
                        df_show['shop_name'].astype(str).str.contains(search_term, case=False)
                    ]
                
                # 格式化日期显示
                st.dataframe(
                    df_show, 
                    hide_index=True, 
                    use_container_width=True,
                    column_config={
                        "follow_up_history": st.column_config.TextColumn("📜 跟进历史 (详细)", width="large"),
                        "last_follow_up_date": st.column_config.DateColumn("上次跟进"),
                        "next_follow_up_date": st.column_config.DateColumn("计划下次"),
                    }
                )

                # 删除功能
                if user_role == 'admin':
                    with st.expander("🗑️ 删除记录"):
                        d_id = st.number_input("ID", min_value=1, key="del")
                        if st.button("确认删除"):
                            delete_data(d_id)
                            st.rerun()

        # 3. 分析页面
        elif choice == "📈 销售分析看板":
            st.subheader("📊 经营数据大屏")
            df = get_data()
            if not df.empty:
                df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce').fillna(0)
                
                # KPI 指标
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("💰 销售总额", f"¥{df['total_amount'].sum():,.0f}")
                c2.metric("📦 订单总量", len(df))
                c3.metric("🔥 成交率", f"{len(df[df['status']=='已完结/已收款']) / len(df) * 100:.1f}%")
                c4.metric("🛑 流失数", len(df[df['purchase_intent']=='流失']))

                st.markdown("---")
                
                c_chart1, c_chart2 = st.columns(2)
                with c_chart1:
                    # 销售龙虎榜
                    rep_perf = df.groupby('sales_rep')['total_amount'].sum().reset_index().sort_values('total_amount', ascending=False)
                    fig = px.bar(rep_perf, x='sales_rep', y='total_amount', text_auto=True, title="🏆 销售龙虎榜 (按金额)", color='sales_rep')
                    st.plotly_chart(fig, use_container_width=True)
                
                with c_chart2:
                    # 客户来源漏斗
                    if 'source' in df.columns:
                        src_counts = df['source'].value_counts().reset_index()
                        src_counts.columns = ['source', 'count']
                        fig2 = px.pie(src_counts, values='count', names='source', title="🌍 客户来源分布", hole=0.4)
                        st.plotly_chart(fig2, use_container_width=True)

if __name__ == '__main__':
    main()