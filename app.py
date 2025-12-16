import streamlit as st
import pandas as pd
import sqlite3
import datetime
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import io
import os 

# --- 核心配置 ---
# 🚨 使用本地文件数据库，确保刷新网页数据不丢失
DB_FILE = 'crm_data.db' 

# --- 初始化与数据结构 ---
INITIAL_USERS = {
    'admin': {'password': 'admin123', 'role': 'admin', 'display_name': '超级管理员'},
    'zhaoxiaoan': {'password': 'zhaoxiaoan123', 'role': 'admin', 'display_name': '赵小安'},
    'liqiufang': {'password': '123', 'role': 'user', 'display_name': '李秋芳'}, 
    'fanqiuju': {'password': '123', 'role': 'user', 'display_name': '范秋菊'},
    'zhoumengke': {'password': '123', 'role': 'user', 'display_name': '周梦珂'},
}

SITE_OPTIONS = [
    "篮球馆（FIBA认证场地）", "排球馆", "羽毛球馆", "乒乓球馆", "室内网球场", "手球馆", "室内足球/五人制足球场",
    "学校体育馆", "幼儿园室内活动室", "小学/中学/大学多功能运动场", "室内操场/风雨操场",
    "综合健身房", "瑜伽馆、舞蹈室", "搏击/武术训练馆", "跨界训练（CrossFit）场地", "体能康复训练中心",
    "社区体育中心", "企事业单位职工活动中心", "商业连锁健身房", "青少年培训机构",
    "轮滑场", "壁球馆", "室内滑冰训练辅助区", "部队、公安、消防训练馆", "医院康复科运动治疗室", "老年活动中心", "其他/未分类"
]
SHOP_OPTIONS = ["天猫旗舰店", "拼多多运动店铺", "拼多多旗舰店", "淘宝店铺", "抖音店铺", "线下渠道/其他"]
STATUS_OPTIONS = ["初次接触", "已寄样", "报价中", "合同流程", "施工中", "已完结/已收款", "流失/搁置", "已流失"]
INTENT_OPTIONS = ["高", "中", "低", "已成交", "流失", "已放弃"]
SOURCE_OPTIONS = ["自然进店", "拼多多推广", "天猫推广", "老客户转介绍", "其他"]
PROMO_TYPE_OPTIONS = ["成交收费", "成交加扣", "其他"]

# 映射字典
CRM_COL_MAP = {
    'id': 'ID', 'date': '录入日期', 'sales_rep': '对接人', 'customer_name': '客户名称',
    'phone': '联系电话', 'source': '客户来源', 'shop_name': '店铺名称', 'unit_price': '单价(元/㎡)',
    'area': '平方数(㎡)', 'site_type': '应用场地', 'status': '跟踪进度', 'is_construction': '是否施工',
    'construction_fee': '施工费(元)', 'material_fee': '辅料费(元)', 'shipping_fee': '运费(元)', 
    'purchase_intent': '购买意向', 'total_amount': '预估总金额(元)', 'follow_up_history': '跟进历史',
    'sample_no': '寄样单号', 'order_no': '订单号', 'last_follow_up_date': '上次跟进日期', 
    'next_follow_up_date': '计划下次跟进'
}
PROMO_COL_MAP = {
    'id': 'ID', 'month': '月份', 'shop': '店铺', 'promo_type': '推广类型',
    'total_spend': '总花费(元)', 'trans_spend': '成交花费(元)', 'net_gmv': '净成交额(元)',
    'net_roi': '净投产比(ROI)', 'cpa_net': '每笔净成交花费(元)', 'inquiry_count': '询单量',
    'inquiry_spend': '询单花费(元)', 'cpl': '询单成本(元/个)', 'note': '备注'
}

CN_TO_EN_MAP = {v: k for k, v in CRM_COL_MAP.items()}
REQUIRED_IMPORT_COLUMNS = list(CRM_COL_MAP.values())[1:] # 排除ID
DATABASE_COLUMNS = list(CRM_COL_MAP.keys())[1:] # 排除ID

# 列名清洗映射
COLUMN_REMAP = {
    '日期': '录入日期', '店铺名字': '店铺名称', '单价（元/㎡）': '单价(元/㎡)', '平方数（㎡）': '平方数(㎡)',
    '应用场地 ': '应用场地', '跟踪进度 ': '跟踪进度', '是否施工 ': '是否施工',
    '施工费（元）': '施工费(元)', '辅料费用（元）': '辅料费(元)', '购买意向 ': '购买意向',
    '总金额（元）': '预估总金额(元)', '备注': '跟进历史', '手机': '联系电话', '电话': '联系电话',
    '客户来源': '客户来源', '运费（元）': '运费(元)',
    '单价(元/m²)': '单价(元/㎡)', '平方数(m²)': '平方数(㎡)', '平方数（m²）': '平方数(㎡)', '总金额(元)': '预估总金额(元)',
}

# --- 数据库连接函数 ---
def get_conn():
    # check_same_thread=False 允许在 Streamlit 的多线程环境中使用
    return sqlite3.connect(DB_FILE, check_same_thread=False)

def init_db():
    conn = get_conn()
    c = conn.cursor()
    
    # 1. 用户表
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY, password TEXT, role TEXT, display_name TEXT
    )''')
    c.execute("SELECT COUNT(*) FROM users")
    if c.fetchone()[0] == 0:
        for u, d in INITIAL_USERS.items():
            c.execute("INSERT OR IGNORE INTO users VALUES (?, ?, ?, ?)", (u, d['password'], d['role'], d['display_name']))
    
    # 2. 销售表 (包含 shipping_fee)
    # 检查列是否存在，不存在则尝试添加(简单迁移逻辑)
    try:
        c.execute("SELECT shipping_fee FROM sales LIMIT 1")
    except:
        c.execute("DROP TABLE IF EXISTS sales") # 简单粗暴：结构不对就重建，确保运行

    c.execute('''CREATE TABLE IF NOT EXISTS sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT, sales_rep TEXT, customer_name TEXT, phone TEXT, source TEXT, shop_name TEXT,
        unit_price REAL, area REAL, site_type TEXT, status TEXT, is_construction TEXT,
        construction_fee REAL, material_fee REAL, shipping_fee REAL, purchase_intent TEXT,
        total_amount REAL, follow_up_history TEXT, sample_no TEXT, order_no TEXT,
        last_follow_up_date TEXT, next_follow_up_date TEXT
    )''')

    # 3. 推广表
    c.execute('''CREATE TABLE IF NOT EXISTS promotions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        month TEXT, shop TEXT, promo_type TEXT, total_spend REAL, trans_spend REAL,
        net_gmv REAL, net_roi REAL, cpa_net REAL, inquiry_count INTEGER,
        inquiry_spend REAL, cpl REAL, note TEXT
    )''')
    conn.commit()
    conn.close()

# --- 核心 CRUD 函数 ---
def get_data(rename_cols=False):
    conn = get_conn()
    try:
        df = pd.read_sql_query("SELECT * FROM sales", conn)
        if rename_cols: df.rename(columns=CRM_COL_MAP, inplace=True)
        return df
    except: return pd.DataFrame()
    finally: conn.close()

def add_data(data):
    conn = get_conn()
    c = conn.cursor()
    placeholders = ', '.join(['?'] * len(DATABASE_COLUMNS))
    c.execute(f"INSERT INTO sales ({', '.join(DATABASE_COLUMNS)}) VALUES ({placeholders})", data)
    conn.commit()
    conn.close()

def update_follow_up(record_id, new_log, next_date, new_status, new_intent):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        UPDATE sales 
        SET follow_up_history = follow_up_history || ?, 
            last_follow_up_date = ?, next_follow_up_date = ?, status = ?, purchase_intent = ?
        WHERE id = ?
    """, (f"\n{new_log}", datetime.date.today().isoformat(), next_date, new_status, new_intent, record_id))
    conn.commit()
    conn.close()

def get_user_info(username):
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT password, role, display_name FROM users WHERE username=?", (username,))
    res = c.fetchone()
    conn.close()
    if res: return {'password': res[0], 'role': res[1], 'display_name': res[2]}
    return None

def get_user_map():
    conn = get_conn()
    df = pd.read_sql_query("SELECT username, display_name FROM users", conn)
    conn.close()
    return df.set_index('username')['display_name'].to_dict()

def get_display_name_to_username_map():
    conn = get_conn()
    df = pd.read_sql_query("SELECT username, display_name FROM users", conn)
    conn.close()
    return df.set_index('display_name')['username'].to_dict()

def add_new_user(username, password, role, display_name):
    conn = get_conn()
    try:
        conn.execute("INSERT INTO users VALUES (?, ?, ?, ?)", (username, password, role, display_name))
        conn.commit()
        return True
    except: return False
    finally: conn.close()

def delete_data(record_id):
    conn = get_conn()
    conn.execute("DELETE FROM sales WHERE id=?", (record_id,))
    conn.commit()
    conn.close()

# 推广数据函数
def get_promo_data(rename_cols=False):
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM promotions", conn)
    conn.close()
    if rename_cols:
        valid_map = {k: v for k, v in PROMO_COL_MAP.items() if k in df.columns}
        df.rename(columns=valid_map, inplace=True)
    return df

def add_promo_data(data):
    conn = get_conn()
    c = conn.cursor()
    c.execute('''INSERT INTO promotions (
        month, shop, promo_type, total_spend, trans_spend, net_gmv, 
        net_roi, cpa_net, inquiry_count, inquiry_spend, cpl, note
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', data)
    conn.commit()
    conn.close()

# 导入功能 (健壮版)
def import_data_from_excel(df_imported):
    conn = get_conn()
    c = conn.cursor()
    user_map_rev = get_display_name_to_username_map()
    
    # 清洗
    df_imported.columns = [col.strip() for col in df_imported.columns]
    df_imported.rename(columns=COLUMN_REMAP, inplace=True)
    
    if '客户名称' not in df_imported.columns:
        return False, "缺少必填列：客户名称"

    df_to_save = df_imported.copy()
    # 补全缺失列
    for col in CN_TO_EN_MAP:
        if col not in df_to_save.columns:
            df_to_save[col] = 0.0 if '费' in col or '价' in col or '平' in col or '额' in col else ''
            
    df_to_save.rename(columns=CN_TO_EN_MAP, inplace=True)
    
    # 格式转换
    num_cols = ['unit_price', 'area', 'construction_fee', 'material_fee', 'shipping_fee', 'total_amount']
    for col in num_cols:
        df_to_save[col] = df_to_save[col].astype(str).str.replace(r'[^\d\.]', '', regex=True)
        df_to_save[col] = pd.to_numeric(df_to_save[col], errors='coerce').fillna(0.0)
        
    df_to_save['sales_rep'] = df_to_save['sales_rep'].astype(str).apply(lambda x: user_map_rev.get(x.strip(), 'admin'))
    
    # 写入
    data_tuples = []
    for _, row in df_to_save.iterrows():
        tup = tuple(row.get(c, '') for c in DATABASE_COLUMNS)
        data_tuples.append(tup)
        
    try:
        placeholders = ','.join(['?'] * len(DATABASE_COLUMNS))
        c.executemany(f"INSERT INTO sales ({','.join(DATABASE_COLUMNS)}) VALUES ({placeholders})", data_tuples)
        conn.commit()
        return True, len(df_imported)
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

# --- 登录 ---
def check_password():
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False
    
    if not st.session_state["password_correct"]:
        st.header("🏢 CRM 系统登录")
        user = st.text_input("用户名")
        pwd = st.text_input("密码", type="password")
        if st.button("登录"):
            uinfo = get_user_info(user)
            if uinfo and uinfo['password'] == pwd:
                st.session_state["password_correct"] = True
                st.session_state["role"] = uinfo['role']
                st.session_state["user_now"] = user
                st.session_state["display_name"] = uinfo['display_name']
                st.rerun()
            else:
                st.error("密码错误")
        return False
    return True

# --- 主程序 ---
def main():
    st.set_page_config(page_title="CRM全能版", layout="wide")
    init_db()

    if check_password():
        user_name = st.session_state["display_name"]
        role = st.session_state["role"]
        user_map = get_user_map()
        
        st.sidebar.title(f"👤 {user_name}")
        menu = ["📝 新增销售记录", "📊 数据追踪与查看", "📈 销售分析看板", "🌐 推广数据看板"]
        choice = st.sidebar.radio("菜单", menu)

        # 侧边栏：备份功能 (保留！)
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 💾 数据备份")
        if st.sidebar.button("下载客户数据 (Excel)"):
            df_exp = get_data(rename_cols=True)
            if not df_exp.empty:
                df_exp['对接人'] = df_exp['对接人'].map(user_map).fillna(df_exp['对接人'])
                out = io.BytesIO()
                with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
                    df_exp.to_excel(writer, index=False)
                st.sidebar.download_button("📥 点击下载备份", data=out.getvalue(), file_name=f'CRM_Backup_{datetime.date.today()}.xlsx')
            else:
                st.sidebar.warning("暂无数据")

        # 1. 新增
        if choice == "📝 新增销售记录":
            st.subheader("📝 录入新客户")
            with st.form("add_form", clear_on_submit=True):
                c1, c2, c3 = st.columns(3)
                date_val = c1.date_input("录入日期", datetime.date.today())
                name = c1.text_input("客户名称 (必填)")
                phone = c1.text_input("电话")
                source = c1.selectbox("来源", SOURCE_OPTIONS)
                
                shop = c2.selectbox("店铺", SHOP_OPTIONS)
                site = c2.selectbox("场地", SITE_OPTIONS)
                price = c2.number_input("单价", 0.0)
                area = c2.number_input("面积", 0.0)
                
                is_const = c3.selectbox("施工", ["否", "是"])
                fee1 = c3.number_input("施工费", 0.0)
                fee2 = c3.number_input("辅料费", 0.0)
                fee3 = c3.number_input("运费", 0.0)
                
                st.markdown("---")
                c4, c5 = st.columns(2)
                intent = c4.selectbox("意向", INTENT_OPTIONS)
                status = c4.selectbox("进度", STATUS_OPTIONS)
                remark = c5.text_area("备注")
                
                if st.form_submit_button("提交录入"):
                    if not name:
                        st.error("请输入客户名称")
                    else:
                        total = (price * area) + fee1 + fee2
                        data = (
                            str(date_val), st.session_state['user_now'], name, phone, source, shop,
                            price, area, site, status, is_const, fee1, fee2, fee3, intent, total,
                            f"[{datetime.date.today()}] {remark}", "", "", str(date_val), ""
                        )
                        add_data(data)
                        st.success("录入成功！")

        # 2. 列表
        elif choice == "📊 数据追踪与查看":
            st.subheader("📋 客户列表")
            df = get_data(rename_cols=True)
            
            # 快速跟进
            with st.expander("➕ 快速追加跟进"):
                if not df.empty:
                    df['显示对接人'] = df['对接人'].map(user_map).fillna(df['对接人'])
                    opts = [f"{r['ID']} - {r['客户名称']} ({r['显示对接人']})" for i, r in df.iterrows()]
                    sel = st.selectbox("选择客户", opts)
                    note = st.text_input("跟进内容")
                    if st.button("提交跟进"):
                        uid = int(sel.split(' - ')[0])
                        update_follow_up(uid, f"[{user_name}]: {note}", str(datetime.date.today()), "跟进中", "中")
                        st.success("已更新")
                        st.rerun()
            
            st.markdown("---")
            if not df.empty:
                # 过滤器
                c1, c2, c3 = st.columns(3)
                filter_user = c1.selectbox("筛选对接人", ["全部"] + list(user_map.values()))
                search = c3.text_input("搜索客户/电话")
                
                df_show = df.copy()
                df_show['对接人'] = df_show['对接人'].map(user_map).fillna(df_show['对接人'])
                
                if filter_user != "全部":
                    df_show = df_show[df_show['对接人'] == filter_user]
                if search:
                    df_show = df_show[df_show['客户名称'].str.contains(search, na=False) | df_show['联系电话'].str.contains(search, na=False)]
                
                st.dataframe(df_show, use_container_width=True, hide_index=True)
            
            # 管理员导入
            if role == 'admin':
                st.markdown("---")
                with st.expander("🛠️ 管理员导入 (Excel/CSV)"):
                    up_file = st.file_uploader("上传文件", type=['xlsx', 'csv'])
                    if up_file:
                        if st.button("确认导入"):
                            try:
                                if up_file.name.endswith('.csv'): df_i = pd.read_csv(up_file)
                                else: df_i = pd.read_excel(up_file)
                                ok, msg = import_data_from_excel(df_i)
                                if ok: 
                                    st.success(f"导入成功 {msg} 条")
                                    st.rerun()
                                else: st.error(msg)
                            except Exception as e: st.error(f"错误: {e}")

        # 3. 销售分析 (保留原貌)
        elif choice == "📈 销售分析看板":
            st.subheader("📈 销售数据分析")
            df = get_data(rename_cols=True)
            if df.empty:
                st.warning("暂无数据")
            else:
                target = st.sidebar.number_input("本月目标", 100000)
                
                # 数据转换
                num_cols = ['预估总金额(元)', '平方数(㎡)', '运费(元)']
                for c in num_cols: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
                
                total_sales = df['预估总金额(元)'].sum()
                total_area = df['平方数(㎡)'].sum()
                
                k1, k2, k3 = st.columns(3)
                k1.metric("总销售额", f"¥{total_sales:,.0f}")
                k2.metric("总面积", f"{total_area:,.0f} ㎡")
                k3.metric("完成率", f"{min(total_sales/target, 1.0)*100:.1f}%")
                
                # 图表
                c1, c2 = st.columns(2)
                fig1 = px.pie(df, names='店铺名称', values='预估总金额(元)', title="店铺业绩占比")
                c1.plotly_chart(fig1, use_container_width=True)
                
                fig2 = px.bar(df, x='客户来源', y='预估总金额(元)', title="客户来源分析", color='客户来源')
                c2.plotly_chart(fig2, use_container_width=True)
                
                # 龙虎榜
                st.markdown("#### 🏆 销售龙虎榜")
                df['对接人'] = df['对接人'].map(user_map).fillna(df['对接人'])
                rank = df.groupby('对接人')['预估总金额(元)'].sum().reset_index().sort_values('预估总金额(元)', ascending=False)
                st.dataframe(rank, use_container_width=True, hide_index=True)

        # 4. 推广看板 (保留原貌)
        elif choice == "🌐 推广数据看板":
            st.subheader("🌐 推广数据")
            dfp = get_promo_data(rename_cols=True)
            
            with st.expander("➕ 录入推广数据"):
                c1, c2 = st.columns(2)
                pm = c1.date_input("月份")
                ps = c1.selectbox("店铺", SHOP_OPTIONS)
                pt = c1.selectbox("类型", PROMO_TYPE_OPTIONS)
                cost = c2.number_input("总花费", 0.0)
                gmv = c2.number_input("成交额", 0.0)
                
                if st.button("提交推广数据"):
                    add_promo_data((str(pm)[:7], ps, pt, cost, 0, gmv, (gmv/cost if cost>0 else 0), 0, 0, 0, 0, ""))
                    st.success("已录入")
                    st.rerun()
            
            if not dfp.empty:
                st.dataframe(dfp, use_container_width=True)
                fig = px.bar(dfp, x='月份', y=['总花费(元)', '净成交额(元)'], barmode='group', title="投入产出对比")
                st.plotly_chart(fig, use_container_width=True)

if __name__ == '__main__':
    main()