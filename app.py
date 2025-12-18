import streamlit as st
import pandas as pd
import sqlite3
import datetime
import plotly.express as px
import io
import os 
from datetime import timedelta

# --- 核心配置 ---
st.set_page_config(page_title="CRM全能版", layout="wide")
DB_FILE = 'crm_data.db' 

# --- 常量定义 ---
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
SHOP_OPTIONS = ["天猫旗舰店", "拼多多运动店铺", "拼多多旗舰店", "天猫德丽士旗舰店", "淘宝店铺", "抖音店铺", "线下渠道/其他"]
STATUS_OPTIONS = ["初次接触", "已寄样", "报价中", "合同流程", "已签约", "施工中", "已完结/已收款", "流失/搁置", "已流失", "方案报价", "样品测试", "价格谈判"]
INTENT_OPTIONS = ["高", "中", "低", "已成交", "流失", "已放弃"]
SOURCE_OPTIONS = ["自然进店", "拼多多推广", "天猫推广", "老客户转介绍", "其他"]
PROMO_TYPE_OPTIONS = ["成交收费", "成交加扣", "其他"]

# 数据库列映射
CRM_COL_MAP = {
    'id': 'ID', 'date': '录入日期', 'sales_rep': '对接人', 'customer_name': '客户名称',
    'phone': '联系电话', 'source': '客户来源', 'shop_name': '店铺名称', 'unit_price': '单价(元/㎡)',
    'area': '平方数(㎡)', 'site_type': '应用场地', 'status': '跟踪进度', 'is_construction': '是否施工',
    'construction_fee': '施工费(元)', 'material_fee': '辅料费(元)', 'shipping_fee': '运费(元)', 
    'purchase_intent': '购买意向', 'total_amount': '预估总金额(元)', 'follow_up_history': '跟进历史',
    'sample_no': '寄样单号', 'order_no': '订单号', 'last_follow_up_date': '上次跟进日期', 
    'next_follow_up_date': '计划下次跟进'
}

DATABASE_COLUMNS = list(CRM_COL_MAP.keys())[1:] # 排除ID

# --- 核心辅助函数 (修复报错的关键) ---

def get_safe_float(value):
    """
    【修复核心 1】: 安全转换数字。
    如果数据库里是 None, '', '¥100', '1,000' 这种乱七八糟的格式，全部转为 float，防止报错。
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        # 清理货币符号和逗号
        cleaned = str(value).replace('¥', '').replace(',', '').replace('$', '').strip()
        if not cleaned:
            return 0.0
        return float(cleaned)
    except:
        return 0.0

def get_safe_date_str(value):
    """
    【修复核心 2】: 安全转换日期字符串。防止 None 导致的比较错误。
    """
    if pd.isna(value) or value == 'None' or value == '':
        return None
    try:
        # 尝试转为 YYYY-MM-DD 格式
        return pd.to_datetime(value).strftime('%Y-%m-%d')
    except:
        return None

# --- 数据库操作 ---
@st.cache_resource
def get_conn():
    return sqlite3.connect(DB_FILE, check_same_thread=False)

def init_db():
    conn = get_conn()
    c = conn.cursor()
    # 用户表
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY, password TEXT, role TEXT, display_name TEXT
    )''')
    # 初始化默认用户
    c.execute("SELECT COUNT(*) FROM users")
    if c.fetchone()[0] == 0:
        for u, d in INITIAL_USERS.items():
            c.execute("INSERT OR IGNORE INTO users VALUES (?, ?, ?, ?)", (u, d['password'], d['role'], d['display_name']))
    
    # 销售记录表 (增加所有字段)
    c.execute('''CREATE TABLE IF NOT EXISTS sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT, sales_rep TEXT, customer_name TEXT, phone TEXT, source TEXT, shop_name TEXT,
        unit_price REAL, area REAL, site_type TEXT, status TEXT, is_construction TEXT,
        construction_fee REAL, material_fee REAL, shipping_fee REAL, purchase_intent TEXT,
        total_amount REAL, follow_up_history TEXT, sample_no TEXT, order_no TEXT,
        last_follow_up_date TEXT, next_follow_up_date TEXT
    )''')
    
    # 推广表
    c.execute('''CREATE TABLE IF NOT EXISTS promotions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        month TEXT, shop TEXT, promo_type TEXT, total_spend REAL, trans_spend REAL,
        net_gmv REAL, net_roi REAL, cpa_net REAL, inquiry_count INTEGER,
        inquiry_spend REAL, cpl REAL, note TEXT
    )''')
    conn.commit()

def get_data(rename_cols=False):
    conn = get_conn()
    try:
        df = pd.read_sql_query("SELECT * FROM sales", conn)
        # 强制处理数字列，防止读取后是字符串导致计算报错
        num_cols = ['unit_price', 'area', 'construction_fee', 'material_fee', 'shipping_fee', 'total_amount']
        for col in num_cols:
            if col in df.columns:
                df[col] = df[col].apply(get_safe_float)
        
        if rename_cols: 
            df.rename(columns=CRM_COL_MAP, inplace=True)
        return df
    except Exception as e:
        return pd.DataFrame()

def get_single_record(record_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM sales WHERE id=?", (record_id,))
    row = c.fetchone()
    if row:
        # 将 tuple 转为 dict
        cols = ['id'] + DATABASE_COLUMNS
        record = dict(zip(cols, row))
        return record
    return None

def add_data(data_tuple):
    conn = get_conn()
    c = conn.cursor()
    placeholders = ', '.join(['?'] * len(DATABASE_COLUMNS))
    sql = f"INSERT INTO sales ({', '.join(DATABASE_COLUMNS)}) VALUES ({placeholders})"
    c.execute(sql, data_tuple)
    conn.commit()

def update_data(record_id, data_dict):
    conn = get_conn()
    c = conn.cursor()
    
    # 确保数字安全转换
    num_cols = ['unit_price', 'area', 'construction_fee', 'material_fee', 'shipping_fee', 'total_amount']
    for col in num_cols:
        if col in data_dict:
            data_dict[col] = get_safe_float(data_dict[col])
            
    # 构建 UPDATE 语句
    set_clause = ", ".join([f"{k}=?" for k in data_dict.keys()])
    values = list(data_dict.values()) + [record_id]
    
    sql = f"UPDATE sales SET {set_clause} WHERE id=?"
    c.execute(sql, values)
    conn.commit()

def update_follow_up(record_id, new_log, next_date, new_status, new_intent):
    conn = get_conn()
    c = conn.cursor()
    # 获取旧日志
    c.execute("SELECT follow_up_history FROM sales WHERE id=?", (record_id,))
    res = c.fetchone()
    old_log = res[0] if res and res[0] else ""
    
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    full_log = f"{old_log}\n[{current_time} {st.session_state.get('display_name', 'System')}]: {new_log}".strip()
    
    c.execute("""
        UPDATE sales 
        SET follow_up_history = ?, 
            last_follow_up_date = ?, next_follow_up_date = ?, status = ?, purchase_intent = ?
        WHERE id = ?
    """, (full_log, datetime.date.today().isoformat(), str(next_date), new_status, new_intent, record_id))
    conn.commit()

def delete_data(record_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM sales WHERE id=?", (record_id,))
    conn.commit()

# --- 用户系统 ---
def check_login():
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False
    
    if not st.session_state["logged_in"]:
        st.header("🏢 CRM 系统登录")
        with st.form("login_form"):
            user = st.text_input("用户名")
            pwd = st.text_input("密码", type="password")
            submit = st.form_submit_button("登录")
            
            if submit:
                conn = get_conn()
                c = conn.cursor()
                c.execute("SELECT password, role, display_name FROM users WHERE username=?", (user,))
                res = c.fetchone()
                if res and res[0] == pwd:
                    st.session_state["logged_in"] = True
                    st.session_state["role"] = res[1]
                    st.session_state["user_now"] = user
                    st.session_state["display_name"] = res[2]
                    st.rerun()
                else:
                    st.error("用户名或密码错误")
        return False
    return True

def get_user_map():
    conn = get_conn()
    df = pd.read_sql("SELECT username, display_name FROM users", conn)
    return dict(zip(df['username'], df['display_name']))

# --- 功能模块 ---

def page_add_sales():
    st.subheader("📝 新增销售记录")
    user_map = get_user_map()
    
    with st.form("add_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        date_val = c1.date_input("录入日期", datetime.date.today())
        name = c1.text_input("客户名称 (必填)")
        phone = c1.text_input("联系电话")
        
        shop = c2.selectbox("店铺名称", SHOP_OPTIONS)
        site = c2.selectbox("应用场地", SITE_OPTIONS)
        source = c2.selectbox("客户来源", SOURCE_OPTIONS)
        
        # 初始录入
        price = c3.number_input("单价(元/㎡)", min_value=0.0, step=1.0)
        area = c3.number_input("平方数(㎡)", min_value=0.0, step=1.0)
        
        st.markdown("---")
        c4, c5 = st.columns(2)
        status = c4.selectbox("跟踪进度", STATUS_OPTIONS)
        intent = c4.selectbox("购买意向", INTENT_OPTIONS)
        is_const = c5.selectbox("是否施工", ["否", "是"])
        
        next_fup = st.date_input("计划下次跟进", datetime.date.today() + datetime.timedelta(days=3))
        remark = st.text_area("首次沟通记录")
        
        submit = st.form_submit_button("提交录入")
        
        if submit:
            if not name:
                st.error("客户名称不能为空！")
            else:
                total = price * area
                log = f"[{datetime.date.today()}] 首次录入: {remark}"
                # 构建数据元组 (注意顺序需与 DATABASE_COLUMNS 一致)
                # date, sales_rep, customer_name, phone, source, shop_name, unit_price, area, site_type ...
                data = (
                    str(date_val), st.session_state['user_now'], name, phone, source, shop,
                    price, area, site, status, is_const, 
                    0.0, 0.0, 0.0, intent, total, log, '', '', 
                    str(date_val), str(next_fup)
                )
                add_data(data)
                st.success("✅ 录入成功！")

def page_view_data():
    st.subheader("📊 数据追踪与查看")
    
    # 1. 读取数据
    df = get_data(rename_cols=True)
    user_map = get_user_map()
    
    if df.empty:
        st.info("暂无数据。")
        return

    # 映射真实姓名
    df['对接人名称'] = df['对接人'].map(user_map).fillna(df['对接人'])
    
    # 2. 权限过滤
    if st.session_state['role'] == 'user':
        df_show = df[df['对接人'] == st.session_state['user_now']].copy()
    else:
        df_show = df.copy() # 管理员看全部

    # 3. 逾期提醒 (彻底修复比较错误)
    st.markdown("### 🔔 跟进提醒")
    # 将 '计划下次跟进' 列转为 datetime 对象，出错转为 NaT
    df_show['next_date_dt'] = pd.to_datetime(df_show['计划下次跟进'], errors='coerce').dt.date
    today = datetime.date.today()
    
    # 筛选过期：日期有效 且 小于今天
    overdue = df_show[
        (df_show['next_date_dt'].notna()) & 
        (df_show['next_date_dt'] < today)
    ]
    
    if not overdue.empty:
        st.error(f"🔴 有 {len(overdue)} 个客户跟进已逾期！")
        st.dataframe(overdue[['ID', '对接人名称', '客户名称', '计划下次跟进', '跟踪进度', '购买意向']], hide_index=True)
    else:
        st.success("✅ 没有逾期的跟进任务。")

    # 4. 列表展示与筛选
    st.markdown("---")
    c1, c2 = st.columns([1, 2])
    with c1:
        if st.session_state['role'] == 'admin':
            filter_rep = st.selectbox("筛选对接人", ["全部"] + list(user_map.values()))
    with c2:
        search_txt = st.text_input("🔍 搜索 (客户名/电话/店铺)")
    
    # 执行筛选
    view_df = df_show.copy()
    if st.session_state['role'] == 'admin' and filter_rep != "全部":
        # 反向查找 username
        target_user = [k for k, v in user_map.items() if v == filter_rep][0]
        view_df = view_df[view_df['对接人'] == target_user]
        
    if search_txt:
        mask = (
            view_df['客户名称'].astype(str).str.contains(search_txt, case=False) |
            view_df['联系电话'].astype(str).str.contains(search_txt, case=False) | 
            view_df['店铺名称'].astype(str).str.contains(search_txt, case=False)
        )
        view_df = view_df[mask]

    # 展示主表
    display_cols = ['ID', '录入日期', '对接人名称', '客户名称', '店铺名称', '单价(元/㎡)', '平方数(㎡)', '预估总金额(元)', '跟踪进度', '购买意向', '计划下次跟进', '跟进历史']
    # 格式化金额列以便阅读
    for money_col in ['预估总金额(元)', '单价(元/㎡)']:
        if money_col in view_df.columns:
            view_df[money_col] = view_df[money_col].apply(lambda x: f"¥{x:,.0f}" if isinstance(x, (int, float)) else x)

    st.dataframe(view_df[display_cols], height=400, hide_index=True)

    # 5. 管理员编辑区 (修复 Line 704 崩溃)
    if st.session_state['role'] == 'admin':
        st.markdown("---")
        st.subheader("🛠️ 管理员编辑/删除")
        
        with st.expander("点击展开编辑面板"):
            edit_id = st.number_input("输入要编辑的客户 ID", min_value=1, step=1)
            
            # 获取原始数据
            record = get_single_record(edit_id)
            
            if record:
                st.markdown(f"**正在编辑: {record['customer_name']} (ID: {edit_id})**")
                
                with st.form(key=f"edit_form_{edit_id}"):
                    # 这里的关键是使用 get_safe_float 包装所有 value
                    c1, c2, c3 = st.columns(3)
                    new_name = c1.text_input("客户名称", record['customer_name'])
                    new_phone = c1.text_input("电话", record['phone'])
                    new_shop = c2.selectbox("店铺", SHOP_OPTIONS, index=SHOP_OPTIONS.index(record['shop_name']) if record['shop_name'] in SHOP_OPTIONS else 0)
                    
                    # 修复：防止报错的核心
                    safe_area = get_safe_float(record['area'])
                    safe_price = get_safe_float(record['unit_price'])
                    safe_fee1 = get_safe_float(record['construction_fee'])
                    
                    new_area = c3.number_input("平方数(㎡)", value=safe_area, min_value=0.0)
                    new_price = c3.number_input("单价", value=safe_price, min_value=0.0)
                    new_fee = c1.number_input("施工费", value=safe_fee1, min_value=0.0)
                    
                    new_status = c2.selectbox("状态", STATUS_OPTIONS, index=STATUS_OPTIONS.index(record['status']) if record['status'] in STATUS_OPTIONS else 0)
                    new_intent = c3.selectbox("意向", INTENT_OPTIONS, index=INTENT_OPTIONS.index(record['purchase_intent']) if record['purchase_intent'] in INTENT_OPTIONS else 0)
                    
                    # 必须有提交按钮
                    save_btn = st.form_submit_button("💾 保存修改")
                    
                    if save_btn:
                        # 计算新总价
                        new_total = (new_price * new_area) + new_fee
                        update_dict = {
                            'customer_name': new_name, 'phone': new_phone, 'shop_name': new_shop,
                            'area': new_area, 'unit_price': new_price, 'construction_fee': new_fee,
                            'status': new_status, 'purchase_intent': new_intent,
                            'total_amount': new_total
                        }
                        update_data(edit_id, update_dict)
                        st.success("修改已保存！")
                        st.rerun()

                # 删除按钮独立在 form 外
                if st.button(f"🗑️ 删除 ID {edit_id}"):
                    delete_data(edit_id)
                    st.warning("已删除")
                    st.rerun()
            else:
                st.warning(f"未找到 ID 为 {edit_id} 的记录")

def page_analysis():
    st.subheader("📈 销售分析看板 (增强版)")
    df = get_data(rename_cols=True)
    
    if df.empty:
        st.warning("暂无数据")
        return

    # 数据预处理
    df['is_signed'] = df['跟踪进度'].apply(lambda x: 1 if x == '已签约' else 0)
    # 确保金额是数字
    df['valid_amount'] = df['预估总金额(元)'].apply(get_safe_float)
    df['valid_area'] = df['平方数(㎡)'].apply(get_safe_float)
    
    # 计算签约部分的金额
    df['signed_amount'] = df.apply(lambda x: x['valid_amount'] if x['is_signed'] == 1 else 0, axis=1)
    df['signed_area'] = df.apply(lambda x: x['valid_area'] if x['is_signed'] == 1 else 0, axis=1)

    # 总体指标
    total_signed_count = df['is_signed'].sum()
    total_signed_money = df['signed_amount'].sum()
    
    k1, k2, k3 = st.columns(3)
    k1.metric("总跟进客户数", len(df))
    k2.metric("已签约客户数", int(total_signed_count))
    k3.metric("签约总金额", f"¥{total_signed_money:,.0f}")
    
    st.markdown("---")
    
    # 表格 1: 对接人分析 (仿Excel截图)
    st.subheader("1. 对接人业绩统计")
    user_map = get_user_map()
    df['对接人名'] = df['对接人'].map(user_map).fillna(df['对接人'])
    
    rep_stats = df.groupby('对接人名').agg(
        跟进项目数=('ID', 'count'),
        已签约数=('is_signed', 'sum'),
        签约金额=('signed_amount', 'sum')
    ).reset_index()
    
    rep_stats['签约率'] = (rep_stats['已签约数'] / rep_stats['跟进项目数']).apply(lambda x: f"{x:.1%}")
    rep_stats['平均客单价'] = (rep_stats['签约金额'] / rep_stats['已签约数']).fillna(0).apply(lambda x: f"¥{x:,.0f}")
    rep_stats['签约金额'] = rep_stats['签约金额'].apply(lambda x: f"¥{x:,.0f}")
    
    st.dataframe(rep_stats, use_container_width=True, hide_index=True)
    
    # 表格 2: 店铺分析 (仿Excel截图)
    st.subheader("2. 店铺渠道转化统计")
    shop_stats = df.groupby('店铺名称').agg(
        项目数量=('ID', 'count'),
        签约数量=('is_signed', 'sum'),
        签约总金额=('signed_amount', 'sum'),
        签约总面积=('signed_area', 'sum')
    ).reset_index()
    
    shop_stats['签约率'] = (shop_stats['签约数量'] / shop_stats['项目数量']).apply(lambda x: f"{x:.1%}")
    shop_stats['平均单价(元/㎡)'] = (shop_stats['签约总金额'] / shop_stats['签约总面积']).fillna(0).round(1)
    shop_stats['平均客单价'] = (shop_stats['签约总金额'] / shop_stats['签约数量']).fillna(0).apply(lambda x: f"¥{x:,.0f}")
    shop_stats['签约总金额'] = shop_stats['签约总金额'].apply(lambda x: f"¥{x:,.0f}")
    
    st.dataframe(shop_stats[['店铺名称', '项目数量', '签约数量', '签约率', '平均单价(元/㎡)', '平均客单价']], use_container_width=True, hide_index=True)


# --- 主程序入口 ---
def main():
    init_db()
    if check_login():
        st.sidebar.title(f"👤 {st.session_state['display_name']}")
        
        # 侧边栏菜单
        menu = st.sidebar.radio("菜单", ["新增销售记录", "数据追踪与查看", "销售分析看板", "推广数据(暂未启用)"])
        
        # 备份下载
        st.sidebar.markdown("---")
        if st.sidebar.button("📥 下载备份数据"):
            df = get_data(rename_cols=True)
            out = io.BytesIO()
            with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False)
            st.sidebar.download_button("点击下载 Excel", data=out.getvalue(), file_name="crm_backup.xlsx")

        if menu == "新增销售记录":
            page_add_sales()
        elif menu == "数据追踪与查看":
            page_view_data()
        elif menu == "销售分析看板":
            page_analysis()
        elif menu == "推广数据(暂未启用)":
            st.info("此模块暂时保留，等待后续需求。")

if __name__ == "__main__":
    main()