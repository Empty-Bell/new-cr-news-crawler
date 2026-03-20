import os
import time
import csv
import logging
import random
import tempfile
import shutil
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timezone, timedelta
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_now_kst():
    """한국 시간(KST, UTC+9)을 반환합니다."""
    return datetime.now(timezone(timedelta(hours=9)))

# 로깅 시간을 KST로 설정
def kst_converter(*args):
    return get_now_kst().timetuple()
logging.Formatter.converter = kst_converter

# ============================================================
# 모든 URL은 기존 개별 크롤러 py 파일에서 정상 동작 확인된 것만 사용
# ============================================================
SUPERCATEGORIES = {
    # --- cr_crawler_tvs.py ---
    "TVs": [
        {"name": "TVs", "url": "https://www.consumerreports.org/electronics-computers/tvs/c28700/"}
    ],
    # --- cr_crawler_refrigerators.py (lines 276-281) ---
    "Refrigerators": [
        {"name": "Top-Freezer Refrigerators", "url": "https://www.consumerreports.org/appliances/refrigerators/top-freezer-refrigerator/c28722/"},
        {"name": "Bottom-Freezer Refrigerators", "url": "https://www.consumerreports.org/appliances/refrigerators/bottom-freezer-refrigerator/c28719/"},
        {"name": "French-Door Refrigerators", "url": "https://www.consumerreports.org/appliances/refrigerators/french-door-refrigerator/c37162/"},
        {"name": "Side-by-Side Refrigerators", "url": "https://www.consumerreports.org/appliances/refrigerators/side-side-refrigerator/c28721/"},
        {"name": "Built-In Refrigerators", "url": "https://www.consumerreports.org/appliances/refrigerators/built-in-refrigerator/c28720/"},
        {"name": "Mini Fridges", "url": "https://www.consumerreports.org/appliances/refrigerators/mini-fridges/c200833/"}
    ],
    # --- cr_crawler_washer.py (lines 379-383) ---
    "Washing Machines": [
        {"name": "Front-Load Washers", "url": "https://www.consumerreports.org/appliances/washing-machines/front-load-washer/c28739/"},
        {"name": "Top-Load Agitator Washers", "url": "https://www.consumerreports.org/appliances/washing-machines/top-load-agitator-washer/c32002/"},
        {"name": "Top-Load HE Washers", "url": "https://www.consumerreports.org/appliances/washing-machines/top-load-he-washer/c37107/"},
        {"name": "Compact Washers", "url": "https://www.consumerreports.org/appliances/washing-machines/compact-washers/c37106/"},
        {"name": "Washer-Dryer Combos", "url": "https://www.consumerreports.org/appliances/washer-dryer-combo/c200858/"}
    ],
    # --- cr_crawler_dryers.py (lines 258-261) ---
    "Clothes Dryers": [
        {"name": "Electric Dryers", "url": "https://www.consumerreports.org/appliances/clothes-dryers/electric-dryer/c30562/"},
        {"name": "Gas Dryers", "url": "https://www.consumerreports.org/appliances/clothes-dryers/gas-dryer/c30563/"},
        {"name": "Compact Dryers", "url": "https://www.consumerreports.org/appliances/clothes-dryers/compact-dryers/c37294/"}
    ],
    # --- cr_crawler_vacuums.py (lines 215-217) ---
    "Vacuums": [
        {"name": "Robotic Vacuums", "url": "https://www.consumerreports.org/appliances/vacuum-cleaners/robotic-vacuum/c35183/"},
        {"name": "Robotic Vacuum and Mop Combos", "url": "https://www.consumerreports.org/appliances/vacuum-cleaners/robotic-vacuum-and-mop-combos/c201152/"},
        {"name": "Cordless Stick Vacuums", "url": "https://www.consumerreports.org/appliances/vacuum-cleaners/cordless-stick-vacuums/c200448/"}
    ],
    # --- cr_crawler_cooktops.py (lines 214-217) ---
    "Cooktops": [
        {"name": "Electric Smoothtop Cooktops", "url": "https://www.consumerreports.org/appliances/cooktops/electric-smoothtop-cooktops/c28688/"},
        {"name": "Electric Induction Cooktops", "url": "https://www.consumerreports.org/appliances/cooktops/electric-induction-cooktops/c200764/"},
        {"name": "Gas Cooktops", "url": "https://www.consumerreports.org/appliances/cooktops/gas-cooktop/c28692/"}
    ],
    # --- cr_crawler_dishwashers.py (line 215) ---
    "Dishwashers": [
        {"name": "Dishwashers", "url": "https://www.consumerreports.org/appliances/dishwashers/c28687/"}
    ],
    # --- cr_crawler_microwaves.py (lines 214-216) ---
    "Microwave Ovens": [
        {"name": "Countertop Microwave Ovens", "url": "https://www.consumerreports.org/appliances/microwave-ovens/countertop-microwave-oven/c28706/"},
        {"name": "Over-the-Range Microwave Ovens", "url": "https://www.consumerreports.org/appliances/microwave-ovens/over-the-range-microwave-oven/c32000/"}
    ],
    # --- cr_crawler_wall_ovens.py (lines 215-216) ---
    "Wall Ovens": [
        {"name": "Electric Wall Ovens", "url": "https://www.consumerreports.org/appliances/wall-ovens/electric-wall-ovens/c28738/"},
        {"name": "Combo Wall Ovens", "url": "https://www.consumerreports.org/appliances/wall-ovens/combo-wall-ovens/c200768/"}
    ],
    # --- cr_crawler_ranges.py (lines 214-219) ---
    "Ranges": [
        {"name": "Electric Ranges", "url": "https://www.consumerreports.org/appliances/ranges/electric-range/c28689/"},
        {"name": "Electric Induction Ranges", "url": "https://www.consumerreports.org/appliances/ranges/electric-induction-ranges/c37181/"},
        {"name": "Electric Coil Ranges", "url": "https://www.consumerreports.org/appliances/ranges/electric-coil-ranges/c37179/"},
        {"name": "Gas Ranges", "url": "https://www.consumerreports.org/appliances/ranges/gas-range/c28694/"},
        {"name": "Pro-Style Ranges", "url": "https://www.consumerreports.org/appliances/ranges/pro-style-ranges/c36820/"}
    ],
    # --- cr_crawler_sound_bars.py (line 215) ---
    "Sound Bars": [
        {"name": "Sound Bars", "url": "https://www.consumerreports.org/electronics-computers/sound-bars/c28698/"}
    ],
    # --- cr_crawler_mobile_pc.py ---
    "Smartphones": [
        {"name": "Cell Phones", "url": "https://www.consumerreports.org/electronics-computers/cell-phones/c28726/"}
    ],
    "Smartwatches": [
        {"name": "Smartwatches and Fitness Trackers", "url": "https://www.consumerreports.org/electronics-computers/smartwatches-fitness-trackers/c201155/"}
    ],
    "Laptops": [
        {"name": "Laptops", "url": "https://www.consumerreports.org/electronics-computers/laptops-chromebooks/laptops/c28701/"}
    ]
}

FILE_PATH_ALL_DATA = "CR_All_Data_Latest.xlsx"
FILE_PATH_REPORT = "CR_Delta_Report.xlsx"

def get_timestamped_filename(base_name):
    """파일명 뒤에 _YYYYMMDDHHMM 형식을 붙입니다. (KST 기준)"""
    ts = get_now_kst().strftime("%y%m%d%H%M")
    name, ext = os.path.splitext(base_name)
    return f"{name}_{ts}{ext}"

# ============================================================
# 드라이버 설정 (공유 프로필 사용하여 로그인 유지)
# ============================================================
def setup_driver(profile_path):
    chrome_options = Options()
    
    # 클라우드(GitHub Actions 등) 환경 대응: 헤드리스 모드 활성화
    if os.getenv("GITHUB_ACTIONS") == "true":
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        logger.info("클라우드 환경 감지: 헤드리스 모드로 실행합니다.")
    
    # 세션 유지를 위해 공통적으로 프로필 디렉토리 적용
    chrome_options.add_argument(f"user-data-dir={profile_path}")

    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("--start-maximized")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {
        "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

# ============================================================
# 제품 목록 확장
# ============================================================
def expand_all_products(driver):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
    time.sleep(1)
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(1)
    initial = len(driver.find_elements(By.CSS_SELECTOR, ".row-product"))
    js = """
    let c=0;
    ['.chart-ratings-wrapper .row-footer button','.chart-wrapper.is-collapsed .row-footer button','.row-footer button'].forEach(s=>{document.querySelectorAll(s).forEach(b=>{try{(b.querySelector('div')||b).click();c++}catch(e){b.click();c++}})});
    document.querySelectorAll('button.btn-expand-toggle, button').forEach(b=>{let t=b.innerText?b.innerText.toLowerCase():'';if(b.classList.contains('btn-expand-toggle')||t.includes('see all')||t.includes('view all')||t.includes('show more')){try{(b.querySelector('div')||b).click();c++}catch(e){b.click();c++}}});
    return c;
    """
    for _ in range(5):
        if driver.execute_script(js) == 0: break
        time.sleep(4)
    final = len(driver.find_elements(By.CSS_SELECTOR, ".row-product"))
    logger.info(f"Products expanded: {initial} → {final}")

def human_type(element, text):
    """실제 사람이 타이핑하는 것처럼 글자별로 약간의 지연을 줍니다."""
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(0.1, 0.3))

def auto_login(driver):
    """.env 파일의 정보를 바탕으로 자동 로그인을 시도합니다."""
    email = os.getenv("CR_EMAIL")
    password = os.getenv("CR_PASSWORD")
    
    if not email or not password:
        logger.warning("CR_EMAIL 또는 CR_PASSWORD 환경 변수가 설정되지 않았습니다.")
        return False

    login_url = "https://secure.consumerreports.org/ec/account/login"
    logger.info("자동 로그인 시도 중...")
    driver.get(login_url)
    
    try:
        wait = WebDriverWait(driver, 20)
        # 로그인 필드 대기
        username_field = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#username")))
        password_field = driver.find_element(By.CSS_SELECTOR, "#password")
        login_button = driver.find_element(By.CSS_SELECTOR, "button.qa-sign-in-button")
        
        # 사람처럼 타이핑
        human_type(username_field, email)
        time.sleep(random.uniform(0.5, 1.2))
        human_type(password_field, password)
        time.sleep(random.uniform(0.5, 1.2))
        
        # 로그인 버튼 클릭
        login_button.click()
        
        # 로그인 완료 대기 및 성공 여부 확인
        # 1. 충분한 대기 시간 부여
        time.sleep(8)
        
        # 2. 다양한 지표로 성공 여부 판단
        login_success = False
        
        # 지표 A: 'Sign In' 버튼(Label)이 사라졌는지 확인
        try:
            sign_in_elements = driver.find_elements(By.CSS_SELECTOR, "label#sign-in-label, .qa-sign-in-button")
            # 요소가 없거나, 있더라도 보이지 않으면 성공 가능성 높음
            if not sign_in_elements or not any(el.is_displayed() for el in sign_in_elements):
                logger.info("성공 지표 A 감지: 'Sign In' 버튼이 사라짐.")
                login_success = True
        except:
            pass

        # 지표 B: 사용자 프로필 아이콘 또는 멤버 전용 요소가 나타났는지 확인
        if not login_success:
            try:
                # 멤버 전용 클래스나 사용자 메뉴 아이콘 확인
                member_elements = driver.find_elements(By.CSS_SELECTOR, ".cda-gnav__member--shown, .cda-gnav__account-menu, [data-gn-signin='true']")
                if any(el.is_displayed() for el in member_elements):
                    logger.info("성공 지표 B 감지: 멤버 전용 UI 요소 확인됨.")
                    login_success = True
            except:
                pass

        # 지표 C: URL 확인 (보조 수단)
        if not login_success:
            curr_url = driver.current_url.lower()
            if "login" not in curr_url and "digital-login" not in curr_url:
                logger.info("성공 지표 C 감지: 로그인 관련 URL이 아님.")
                login_success = True

        if login_success:
            logger.info("자동 로그인 성공 확인 완료!")
            return True
        else:
            logger.warning(f"로그인 성공 여부를 확신할 수 없습니다. (현재 URL: {driver.current_url})")
            return False
            
    except Exception as e:
        logger.error(f"자동 로그인 도중 에러 발생: {e}")
        return False

# ============================================================
# 데이터 추출 (정상 동작 확인된 JS 그대로 사용)
# ============================================================
def extract_ratings(driver):
    js_extract = """
    let all_data = [];
    let seen_products = new Set();
    let global_headers_info = [];
    let seen_names = new Set();

    let wrappers = Array.from(document.querySelectorAll('.chart-ratings-wrapper'))
                        .filter(w => w.offsetWidth > 0 && w.offsetHeight > 0);
    if (wrappers.length === 0) wrappers = Array.from(document.querySelectorAll('.chart-ratings-wrapper'));
    if (wrappers.length === 0) return [[], []];

    wrappers.forEach(wrapper => {
        let header_row = wrapper.querySelector('.row-header') || document.querySelector('.row-header');
        if (!header_row) return;
        let header_cells = header_row.querySelectorAll('.cell');
        header_cells.forEach((cell, i) => {
            let h = cell.getAttribute('aria-label') || cell.innerText.trim();
            if (!h) { let t = cell.querySelector('.icon__tooltip'); if (t) h = t.getAttribute('aria-label') || t.getAttribute('data-title'); }
            if (!h || h === 'Add to Compare' || h.toLowerCase().includes('green choice')) return;
            h = h.replace(/\\n/g, ' ').trim();
            if (!seen_names.has(h)) { global_headers_info.push({name: h}); seen_names.add(h); }
        });
    });

    let final_headers = global_headers_info.map(hi => hi.name);

        wrappers.forEach(wrapper => {
            let local_headers_info = [];
            let header_row = wrapper.querySelector('.row-header') || document.querySelector('.row-header');
            if (header_row) {
                let cells = header_row.querySelectorAll('.cell');
                cells.forEach((cell, i) => {
                    let h = cell.getAttribute('aria-label') || cell.innerText.trim();
                    if (!h) { let t = cell.querySelector('.icon__tooltip'); if (t) h = t.getAttribute('aria-label') || t.getAttribute('data-title'); }
                    if (!h) return;
                    h = h.replace(/\\n/g, ' ').trim();
                    local_headers_info.push({index: i, name: h});
                });
            }

            let product_rows = wrapper.querySelectorAll('.row-product');
            
            // 현재 섹션의 SubCategory 추출
            let subCatElem = wrapper.parentElement.querySelector('[id="chart-ratings__details"]') 
                          || wrapper.querySelector('[id="chart-ratings__details"]')
                          || document.getElementById('chart-ratings__details');
            let foundSubCat = subCatElem ? subCatElem.innerText.trim() : "";
            
            // SubCategory별로 랭킹 1위부터 시작
            let rank = 1;

            product_rows.forEach(row => {
                let pid = row.getAttribute('data-id') || row.innerText.substring(0, 30);
                if (seen_products.has(pid)) return;
                seen_products.add(pid);

                let rd = {
                    'Rank': rank++,
                    'SubCategory': foundSubCat
                };

            let cells = row.querySelectorAll('.cell');
            local_headers_info.forEach(lhi => {
                if (lhi.name === 'Add to Compare' || lhi.name.toLowerCase().includes('green choice')) return;
                if (lhi.index >= cells.length) return;
                let cell = cells[lhi.index];
                let val = "";
                let ds = cell.querySelector('[data-score]');
                if (ds) { val = ds.getAttribute('data-score'); }
                else {
                    let h4 = cell.querySelector('h4');
                    if (h4) { val = h4.innerText.trim(); }
                    else {
                        let lb = cell.querySelector('label');
                        if (lb && lb.getAttribute('data-score')) { val = lb.getAttribute('data-score'); }
                        else { val = cell.innerText.trim().replace(/\\s+/g, ' '); }
                    }
                }
                if (lhi.name === 'Price') {
                    if (val.includes('Shop')) val = val.split('Shop')[0].trim();
                    val = val.replace(/from/gi, '').replace(/\\$/g, '').replace(/,/g, '').trim();
                }
                rd[lhi.name] = val;
            });
            if (Object.keys(rd).length > 1) all_data.push(rd);
        });
    });

    if (!final_headers.includes('SubCategory')) final_headers.unshift('SubCategory');
    if (!final_headers.includes('Rank')) final_headers.unshift('Rank');
    return [final_headers, all_data];
    """
    try:
        return driver.execute_script(js_extract)
    except Exception as e:
        logger.error(f"Extraction error: {e}")
        return [], []

# ============================================================
# Delta 비교 분석
# ============================================================
def generate_delta_report_v2(old_df, new_df, sc_name):
    """
    특정 SuperCategory(시트)에 대한 델타 리포트 정보를 리스트 형태로 반환합니다.
    """
    changes = {
        "Brand_Metrics": [],
        "Lab_Test_Changes": [],
        "Score_Only_Changes": [],
        "Column_Config": [],
        "Model_Added": [],
        "Model_Deleted": [],
        "Rank1_Changes": []
    }

    if old_df.empty and new_df.empty:
        return changes

    # 공통 비교 키
    KEY_COLS = ['SuperCategory', 'Category', 'SubCategory', 'Brand', 'Product']
    BRAND_METRIC_COLS = ["Brand Reliability", "Owner Satisfaction"]
    # 비교에서 제외할 메타 컬럼들
    SKIP_COLS = set(KEY_COLS) | {'Rank', 'Overall Score', 'Price', 'Extracted_At', 'n_rank', 'numeric_rank'}
    TARGET_BRANDS = ['SAMSUNG', 'DACOR']

    # 비교를 위해 인덱스 설정
    def prepare_df(df):
        if df.empty: return pd.DataFrame(columns=KEY_COLS).set_index(KEY_COLS)
        d = df.copy()
        # 키 컬럼 정규화
        for c in KEY_COLS: 
            if c in d.columns:
                d[c] = d[c].fillna('').astype(str).str.strip()
            else:
                d[c] = ''
        return d.drop_duplicates(subset=KEY_COLS).set_index(KEY_COLS)

    old_m = prepare_df(old_df)
    new_m = prepare_df(new_df)

    all_cols = list(set(old_m.columns) | set(new_m.columns))
    # 브랜드 지표 유연한 매칭
    actual_brand_rel = next((c for c in all_cols if "Brand Reliability" in c), None)
    actual_owner_sat = next((c for c in all_cols if "Owner Satisfaction" in c), None)
    brand_cols = [c for c in [actual_brand_rel, actual_owner_sat] if c]

    # Lab Test 대상 컬럼들 (브랜드 메트릭 및 요약 컬럼 제외)
    lab_test_cols = [c for c in all_cols if c not in SKIP_COLS and c not in BRAND_METRIC_COLS 
                      and not c.startswith("Samsung_") and not c.startswith("Best_")]

    # 교집합(기존 모델) 비교
    common_idx = old_m.index.intersection(new_m.index)
    for idx in common_idx:
        row_old = old_m.loc[idx]
        row_new = new_m.loc[idx]
        sc, cat, sub, brand, prod = idx

        # Case 1: Brand Metrics
        b_changed = []
        for bc in brand_cols:
            if bc in row_old and bc in row_new:
                vo, vn = str(row_old[bc]).strip(), str(row_new[bc]).strip()
                # 빈 값 정규화
                vo_n = "" if vo.lower() in ["nan", "none", "", "n/a", "-", "na", "null"] else vo
                vn_n = "" if vn.lower() in ["nan", "none", "", "n/a", "-", "na", "null"] else vn
                
                if vo_n != vn_n:
                    changes["Brand_Metrics"].append({"SuperCategory": sc, "Category": cat, "SubCategory": sub, "Brand": brand, "Attribute": bc, "Previous": vo_n, "New": vn_n})
                    b_changed.append(bc)

        # Case 2: Lab Test Evaluation
        l_changed = []
        for lc in lab_test_cols:
            if lc in row_old and lc in row_new:
                vo, vn = str(row_old[lc]).strip(), str(row_new[lc]).strip()
                # 빈 값 정규화
                vo_n = "" if vo.lower() in ["nan", "none", "", "n/a", "-", "na", "null"] else vo
                vn_n = "" if vn.lower() in ["nan", "none", "", "n/a", "-", "na", "null"] else vn
                
                if vo_n != vn_n:
                    # 수치 오차 무시 로직 (둘 다 숫자일 때만)
                    try:
                        if float(vo_n) == float(vn_n): continue
                    except: pass
                    changes["Lab_Test_Changes"].append({"SuperCategory": sc, "Category": cat, "SubCategory": sub, "Brand": brand, "Product": prod, "Attribute": lc, "Previous": vo_n, "New": vn_n})
                    l_changed.append(lc)
        
        # Case 3: Overall Score Only Change (1, 2번 변동이 없을 때만)
        if not b_changed and not l_changed:
            vo_s = str(row_old.get('Overall Score', '')).strip()
            vn_s = str(row_new.get('Overall Score', '')).strip()
            vo_s = "" if vo_s.lower() in ["nan", "none", "", "n/a", "-", "na", "null"] else vo_s
            vn_s = "" if vn_s.lower() in ["nan", "none", "", "n/a", "-", "na", "null"] else vn_s
            
            if vo_s != vn_s:
                changes["Score_Only_Changes"].append({"SuperCategory": sc, "Category": cat, "SubCategory": sub, "Brand": brand, "Product": prod, "Previous Score": vo_s, "New Score": vn_s})

    # Case 4: Category별 컬럼 구성 변경
    if not new_df.empty and not old_df.empty:
        for (cat, sub), g_new in new_df.groupby(['Category', 'SubCategory']):
            g_old = old_df[(old_df['Category']==cat) & (old_df['SubCategory']==sub)]
            if not g_old.empty:
                # 실제로 해당 카테고리 기기에 값이 존재하는 컬럼들만 비교 대상으로 추출
                new_full_cols = [c for c in g_new.columns if g_new[c].notna().any()]
                old_full_cols = [c for c in g_old.columns if g_old[c].notna().any()]

                added = set(new_full_cols) - set(old_full_cols)
                removed = set(old_full_cols) - set(new_full_cols)
                added = [a for a in added if a not in SKIP_COLS and not a.startswith("Samsung_") and not a.startswith("Best_")]
                removed = [r for r in removed if r not in SKIP_COLS and not r.startswith("Samsung_") and not r.startswith("Best_")]
                for a in added: changes["Column_Config"].append({"SuperCategory": sc_name, "Category": cat, "SubCategory": sub, "Attribute": a, "Change": "Added"})
                for r in removed: changes["Column_Config"].append({"SuperCategory": sc_name, "Category": cat, "SubCategory": sub, "Attribute": r, "Change": "Removed"})

    # Case 5/6: 전 브랜드 신규 및 삭제 (삼성/LG 하이라이트)
    HIGHLIGHT_BRANDS = ['SAMSUNG', 'LG', 'DACOR']
    
    for _, row in new_df.iterrows():
        brand = str(row.get('Brand','')).strip()
        key = (str(row.get('SuperCategory','')), str(row.get('Category','')), str(row.get('SubCategory','')), brand, str(row.get('Product','')))
        if old_m.empty or key not in old_m.index:
            display_brand = brand
            if brand.upper() in HIGHLIGHT_BRANDS:
                display_brand = f"★ {brand}"
            changes["Model_Added"].append({
                "SuperCategory": row.get('SuperCategory'), "Category": row.get('Category'), 
                "SubCategory": row.get('SubCategory'), "Rank": row.get('Rank'), 
                "Brand": display_brand, "Overall Score": row.get('Overall Score'), 
                "Product": row.get('Product')
            })

    if not old_df.empty:
        for _, row in old_df.iterrows():
            brand = str(row.get('Brand','')).strip()
            key = (str(row.get('SuperCategory','')), str(row.get('Category','')), str(row.get('SubCategory','')), brand, str(row.get('Product','')))
            if new_m.empty or key not in new_m.index:
                display_brand = brand
                if brand.upper() in HIGHLIGHT_BRANDS:
                    display_brand = f"★ {brand}"
                changes["Model_Deleted"].append({
                    "SuperCategory": row.get('SuperCategory'), "Category": row.get('Category'), 
                    "SubCategory": row.get('SubCategory'), "Previous Rank": row.get('Rank'), 
                    "Brand": display_brand, "Previous Overall Score": row.get('Overall Score'), 
                    "Product": row.get('Product')
                })

    # Case 7: Rank 1 변경
    if not new_df.empty and not old_df.empty:
        temp_new = new_df.copy()
        temp_old = old_df.copy()
        temp_new['n_rank'] = pd.to_numeric(temp_new['Rank'], errors='coerce')
        temp_old['n_rank'] = pd.to_numeric(temp_old['Rank'], errors='coerce')

        for (cat, sub), g_new in temp_new.groupby(['Category', 'SubCategory']):
            g_old = temp_old[(temp_old['Category']==cat) & (temp_old['SubCategory']==sub)]
            if g_old.empty: continue
            
            n1 = g_new[g_new['n_rank']==1]
            o1 = g_old[g_old['n_rank']==1]
            if not n1.empty and not o1.empty:
                n1_brand = str(n1['Brand'].iloc[0])
                n1_prod = str(n1['Product'].iloc[0])
                o1_brand = str(o1['Brand'].iloc[0])
                o1_prod = str(o1['Product'].iloc[0])
                
                if n1_prod != o1_prod:
                    # 기존 1위 현재
                    cur_o1 = g_new[g_new['Product']==o1_prod]
                    c_rank = cur_o1['Rank'].iloc[0] if not cur_o1.empty else "Out of Rank"
                    c_score = cur_o1['Overall Score'].iloc[0] if not cur_o1.empty else "N/A"
                    # 신규 1위 과거
                    pre_n1 = g_old[g_old['Product']==n1_prod]
                    p_rank = pre_n1['Rank'].iloc[0] if not pre_n1.empty else "New Entry"
                    p_score = pre_n1['Overall Score'].iloc[0] if not pre_n1.empty else "N/A"
                    
                    changes["Rank1_Changes"].append({
                        "SuperCategory": sc_name, "Category": cat, "SubCategory": sub,
                        "Previous 1st Brand": o1_brand, "Previous 1st Product": o1_prod,
                        "Current 1st Brand": n1_brand, "Current 1st Product": n1_prod,
                        "Old 1st Current Rank": c_rank, "Old 1st Current Score": c_score,
                        "New 1st Previous Rank": p_rank, "New 1st Previous Score": p_score
                    })
    return changes

def generate_summary(all_data):
    """SuperCategory, Category, SubCategory별로 Samsung/Dacor 및 Best 브랜드 요약 정보를 생성합니다."""
    summary_list = []
    for sc, records in all_data.items():
        if not records: continue
        df = pd.DataFrame(records)
        
        # Rank를 숫자로 변환 (순위 비교용)
        df['numeric_rank'] = pd.to_numeric(df['Rank'], errors='coerce')
        
        # 유연한 컬럼 매칭 (Brand Reliability, Owner Satisfaction)
        cols = df.columns
        rel_col = next((c for c in cols if "Brand Reliability" in c), None)
        sat_col = next((c for c in cols if "Owner Satisfaction" in c), None)

        # Category와 SubCategory로 그룹핑
        df['SubCategory'] = df['SubCategory'].fillna('')
        groups = df.groupby(['Category', 'SubCategory'], sort=False)
        
        for (cat, subcat), group in groups:
            # 1. 해당 그룹 최고의 제품 (Rank 1)
            best_row = group[group['numeric_rank'] == 1]
            if best_row.empty:
                best_row = group.nsmallest(1, 'numeric_rank')
            
            best_brand = best_row['Brand'].iloc[0] if not best_row.empty else "N/A"
            best_prod = best_row['Product'].iloc[0] if not best_row.empty else "N/A"
            best_score = best_row['Overall Score'].iloc[0] if not best_row.empty else "N/A"
            best_rel = best_row[rel_col].iloc[0] if rel_col and not best_row.empty else ""
            best_sat = best_row[sat_col].iloc[0] if sat_col and not best_row.empty else ""
            
            # 2. Samsung 또는 Dacor 제품 중 최고 순위 찾기
            samsung_dacor = group[group['Brand'].astype(str).str.upper().isin(['SAMSUNG', 'DACOR'])]
            if not samsung_dacor.empty:
                samsung_best = samsung_dacor.nsmallest(1, 'numeric_rank').iloc[0]
                s_rank = samsung_best['Rank']
                s_prod = samsung_best['Product'] if 'Product' in samsung_best else ""
                s_score = samsung_best['Overall Score']
                s_rel = samsung_best[rel_col] if rel_col else ""
                s_sat = samsung_best[sat_col] if sat_col else ""
            else:
                s_rank = ""
                s_prod = ""
                s_score = ""
                s_rel = ""
                s_sat = ""
            
            summary_list.append({
                "SuperCategory": sc,
                "Category": cat,
                "SubCategory": subcat,
                "Samsung_Rank": s_rank,
                "Samsung_Product": s_prod,
                "Samsung_Overall Score": s_score,
                "Samsung_Brand Reliability": s_rel,
                "Samsung_Owner Satisfaction": s_sat,
                "Best_Brand": best_brand,
                "Best_Product": best_prod,
                "Best_Overall Score": best_score,
                "Best_Brand Reliability": best_rel,
                "Best_Owner Satisfaction": best_sat
            })
    
    return pd.DataFrame(summary_list)

# ============================================================
# 체크포인트 저장
# ============================================================
def save_checkpoint(data_dict, file_path, prev_data):
    summary_df = generate_summary(data_dict)
    try:
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            # 1. Summary 시트 추가 (가장 앞)
            if not summary_df.empty:
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # 2. 나머지 슈퍼카테고리별 시트
            for sc, records in data_dict.items():
                if not records:
                    if prev_data and sc in prev_data:
                        prev_data[sc].to_excel(writer, sheet_name=sc, index=False)
                    continue
                df = pd.DataFrame(records)
                cols = list(df.columns)
                order = ['SuperCategory', 'Category', 'SubCategory', 'Rank', 'Brand', 'Product', 'Overall Score', 'Price', 'Extracted_At']
                final = [c for c in order if c in cols] + [c for c in cols if c not in order]
                
                # 'Price' 컬럼을 숫자로 변환 (이미 JS에서 처리했으나 한번 더 보장)
                if 'Price' in df.columns:
                    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
                
                df[final].to_excel(writer, sheet_name=sc, index=False)
                
                # openpyxl을 사용하여 가격 컬럼에 통화 서식 적용
                workbook = writer.book
                worksheet = writer.sheets[sc]
                
                # 'Price' 컬럼 인덱스 찾기 (1-based)
                price_idx = None
                for idx, col_name in enumerate(final, 1):
                    if col_name == 'Price':
                        price_idx = idx
                        break
                
                if price_idx:
                    # 데이터 행들에 서식 적용 (헤더 제외)
                    for row in range(2, len(df) + 2):
                        cell = worksheet.cell(row=row, column=price_idx)
                        cell.number_format = '$#,##0.00'
    except Exception as e:
        logger.error(f"Checkpoint save error: {e}")

def send_email_report(all_data, delta_results, extract_time, data_file, report_file):
    """크롤링 결과를 이메일로 송부합니다."""
    smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", 587))
    sender_email = os.getenv("SENDER_EMAIL")
    sender_password = os.getenv("SENDER_PASSWORD")
    receiver_email = "jongbin.yun@samsung.com"

    if not sender_email or not sender_password:
        return

    # 요약 정보 계산
    total_categories = sum(len(v) for v in SUPERCATEGORIES.values())
    collected_count = sum(len(records) for records in all_data.values())
    
    # 1. Category Summary
    summary_df = generate_summary(all_data)
    summary_html = ""
    if not summary_df.empty:
        summary_html = "<h4>[Category Summary]</h4>"
        tbl_html = summary_df.to_html(index=False, border=1, justify='center', na_rep='')
        tbl_html = tbl_html.replace('<table', '<table style="border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; font-size: 11px; margin-bottom: 20px;"')
        
        # 헤더별 배경색 지정 (삼성: 파랑, Best: 빨강, 나머지: 회색)
        for col in summary_df.columns:
            bg_color = "#666666" # 중립
            if "Samsung" in col:
                bg_color = "#004684" # 파랑
            elif "Best" in col:
                bg_color = "#B22222" # 빨강
            
            target_th = f'<th>{col}</th>'
            replace_th = f'<th style="background-color: {bg_color}; color: white; border: 1px solid #ddd; padding: 8px; text-align: center;">{col}</th>'
            tbl_html = tbl_html.replace(target_th, replace_th)

        tbl_html = tbl_html.replace('<td', '<td style="border: 1px solid #ddd; padding: 6px; text-align: center;"')
        # 본문 내 브랜드명 강조 (색상 없이 볼드체만)
        tbl_html = tbl_html.replace('Samsung', '<b>Samsung</b>').replace('Dacor', '<b>Dacor</b>')
        summary_html += tbl_html

    # 2. Delta Report Summary
    delta_html = "<h4>[Delta Report Summary]</h4>"
    delta_exist = False
    for title, df in delta_results.items():
        if df is not None and not df.empty:
            delta_exist = True
            delta_html += f"<b>* {title}</b>"
            # 시인성 좋은 테이블 스타일링
            t_html = df.to_html(index=False, border=1, justify='center', na_rep='')
            t_html = t_html.replace('<table', '<table style="border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; font-size: 10px; margin-bottom: 15px;"')
            t_html = t_html.replace('<th', '<th style="background-color: #555; color: white; border: 1px solid #ddd; padding: 5px;"')
            t_html = t_html.replace('<td', '<td style="border: 1px solid #ddd; padding: 4px; text-align: center;"')
            
            # 삼성/LG 하이라이트 표시 (★를 빨간색으로, 브랜드명 볼드 처리)
            t_html = t_html.replace('★', '<span style="color: #ff0000; font-weight: bold;">★</span>')
            t_html = t_html.replace('SAMSUNG', '<b>SAMSUNG</b>').replace('LG', '<b>LG</b>').replace('DACOR', '<b>DACOR</b>')
            
            delta_html += t_html
    
    if not delta_exist:
        delta_html += "<p>변동 사항 없음</p>"

    body = f"""
    <h3>Consumer Report 크롤링 결과 요약</h3>
    <ul>
        <li><b>수행 일시:</b> {extract_time}</li>
        <li><b>수집 데이터양:</b> 총 {collected_count}개 모델</li>
        <li><b>성공률:</b> {collected_count}개 모델 수집됨 (대상 카테고리: {total_categories}개)</li>
    </ul>

    {delta_html}
    {summary_html}
    
    <p>상세 내용은 첨부된 파일을 확인해 주세요.</p>
    """
    
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = f"[CR Crawl] 결과 요약 ({extract_time})"
    msg.attach(MIMEText(body, 'html'))

    for f_path in [data_file, report_file]:
        if os.path.exists(f_path):
            with open(f_path, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename= {os.path.basename(f_path)}")
            msg.attach(part)

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
        logger.info(f"이메일 리포트 발송 완료: {receiver_email}")
    except Exception as e:
        logger.error(f"이메일 발송 에러: {e}")

# ============================================================
# MAIN
# ============================================================
def main():
    profile_dir = tempfile.mkdtemp()
    logger.info(f"Persistent Session Profile: {profile_dir}")

    all_data = {sc: [] for sc in SUPERCATEGORIES}
    prev_data = {}
    
    # 이전 데이터 로드 로직 강화 및 로깅 추가
    abs_data_path = os.path.abspath(FILE_PATH_ALL_DATA)
    if os.path.exists(abs_data_path):
        logger.info(f"이전 데이터 발견: {abs_data_path}")
        try:
            xl = pd.ExcelFile(abs_data_path)
            for s in xl.sheet_names:
                if s == 'Summary': continue # 요약 시트는 비교 대상에서 제외
                df_loaded = xl.parse(s)
                prev_data[s] = df_loaded
                logger.info(f" - [{s}] 시트 로드 완료: {len(df_loaded)}개 모델")
        except Exception as e:
            logger.error(f"이전 데이터 파싱 에러: {e}")
    else:
        logger.warning(f"이전 데이터 파일을 찾을 수 없습니다: {abs_data_path}")
        logger.warning("모든 데이터가 '신규 모델'로 인식됩니다.")

    extract_time = get_now_kst().strftime("%Y-%m-%d %H:%M:%S")

    # Step 1: 로그인 세션 생성
    first_url = list(SUPERCATEGORIES.values())[0][0]["url"]
    driver = setup_driver(profile_dir)
    
    # 자동 로그인 시도
    login_success = auto_login(driver)
    
    if not login_success:
        logger.info("\n========================================================")
        logger.info("  [!] 자동 로그인 실패 또는 정보 없음. 수동 로그인이 필요합니다.")
        logger.info("  현재 브라우저에서 로그인 완료 후 터미널에서 [Enter]를 눌러주세요.")
        logger.info("========================================================\n")
        driver.get(first_url)
        input("로그인 완료 후 Enter키를 누르세요...")
    
    driver.quit()
    time.sleep(3)

    # Step 2: 슈퍼카테고리별 브라우저 세션 분리
    for sc_name, subcats in SUPERCATEGORIES.items():
        logger.info(f"\n{'='*50}")
        logger.info(f" [{sc_name}] 새 브라우저 세션 시작")
        logger.info(f"{'='*50}")

        driver = setup_driver(profile_dir)
        try:
            for cat in subcats:
                cn = cat["name"]
                cu = cat["url"]
                logger.info(f"\n--- {cn} ---")
                driver.get(cu)
                time.sleep(4)

                # URL 검증 (리다이렉트 방지)
                actual = driver.current_url.split('?')[0].rstrip('/')
                expected = cu.split('?')[0].rstrip('/')
                if actual != expected:
                    logger.warning(f"URL Mismatch! Expected: {expected}")
                    logger.warning(f"              Actual:   {actual}")
                    logger.warning("리다이렉트 감지됨. 이 카테고리를 건너뜁니다.")
                    continue

                try:
                    WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CLASS_NAME, "chart-wrapper")))
                except TimeoutException:
                    logger.warning(f"Timeout: {cn}. 건너뜁니다.")
                    continue

                expand_all_products(driver)
                headers, data = extract_ratings(driver)

                if data:
                    KNOWN_BRANDS = [
                        'Fisher & Paykel', 'Arctic King', 'Dirt Devil', 'Speed Queen',
                        'Magic Chef', 'Unique Appliances', 'Summit Appliances', 'Commercial Chef',
                        'GE Profile', 'GE Monogram', 'GE Café', 'GE Cafe',
                        'Kenmore Elite', 'Kenmore Pro', 'Harman Kardon', 'Open Box',
                        'Black+Decker', 'Sub-Zero'
                    ]

                    for row in data:
                        row["SuperCategory"] = sc_name
                        row["Category"] = cn
                        row["Extracted_At"] = extract_time
                        
                        if "Product" in row:
                            prod_name = str(row["Product"]).strip()
                            brand = None
                            model = None
                            
                            # 특별 케이스 처리 (LG Signature, LG Studio)
                            if prod_name.upper().startswith("LG SIGNATURE"):
                                brand = "LG"
                                model = prod_name[len("LG SIGNATURE"):].strip()
                            elif prod_name.upper().startswith("LG STUDIO"):
                                brand = "LG"
                                model = prod_name[len("LG STUDIO"):].strip()
                            else:
                                # 알려진 복합 명칭 브랜드 검색
                                for kb in KNOWN_BRANDS:
                                    if prod_name.upper().startswith(kb.upper()):
                                        brand = kb
                                        model = prod_name[len(kb):].strip()
                                        break
                                
                                # 매칭되는 게 없으면 첫 번째 단어를 띄어쓰기 기준으로 자르기
                                if not brand:
                                    brand = prod_name.split(' ')[0]
                                    model = prod_name[len(brand):].strip()
                            
                            row["Brand"] = brand
                            row["Product"] = model

                    all_data[sc_name].extend(data)
                    logger.info(f"✅ {cn}: {len(data)}개 수집 완료")
                else:
                    logger.warning(f"❌ {cn}: 데이터 없음")

                delay = random.uniform(8, 15)
                logger.info(f"대기 {delay:.1f}초...")
                time.sleep(delay)

            # 슈퍼카테고리 완료 → 체크포인트
            save_checkpoint(all_data, FILE_PATH_ALL_DATA, prev_data)
            logger.info(f"[{sc_name}] 체크포인트 저장 완료")

        except Exception as e:
            logger.error(f"[{sc_name}] 에러: {e}")
        finally:
            driver.quit()
            pause = random.uniform(8, 15)
            logger.info(f"브라우저 종료. {pause:.1f}초 대기 후 다음 세션...")
            time.sleep(pause)

    # Step 3: Delta Report
    logger.info("\nDelta 리포트 생성 중...")
    
    delta_results = {
        "Brand_Metrics": [],
        "Lab_Test_Changes": [],
        "Score_Only_Changes": [],
        "Column_Config": [],
        "Model_Added": [],
        "Model_Deleted": [],
        "Rank1_Changes": []
    }

    for sc_name in SUPERCATEGORIES.keys():
        df_new = pd.DataFrame(all_data.get(sc_name, []))
        df_old = prev_data.get(sc_name, pd.DataFrame())
        
        if not df_new.empty or not df_old.empty:
            logger.info(f"Comparing {sc_name}...")
            sheet_delta = generate_delta_report_v2(df_old, df_new, sc_name)
            for key in delta_results.keys():
                delta_results[key].extend(sheet_delta[key])

    # 리스트를 데이터프레임으로 변환
    for key in delta_results.keys():
        delta_results[key] = pd.DataFrame(delta_results[key])
        if not delta_results[key].empty:
            delta_results[key] = delta_results[key].drop_duplicates()

    # 1. 고정 파일명으로 저장
    save_checkpoint(all_data, FILE_PATH_ALL_DATA, prev_data)
    
    # Delta Report 엑셀 저장 (멀티 시트)
    changes_found = any(not df.empty for df in delta_results.values())
    try:
        with pd.ExcelWriter(FILE_PATH_REPORT, engine='openpyxl') as writer:
            if changes_found:
                for sheet_name, df_delta in delta_results.items():
                    if not df_delta.empty:
                        df_delta.to_excel(writer, sheet_name=sheet_name, index=False)
            else:
                pd.DataFrame([{"Message": "변경사항 없음", "Checked_At": extract_time}]).to_excel(writer, sheet_name="No_Changes", index=False)
    except Exception as e:
        logger.error(f"Delta Report save error: {e}")

    # 2. 타임스탬프 파일명으로 저장
    ts_data_file = get_timestamped_filename(FILE_PATH_ALL_DATA)
    ts_report_file = get_timestamped_filename(FILE_PATH_REPORT)
    
    shutil.copy(FILE_PATH_ALL_DATA, ts_data_file)
    shutil.copy(FILE_PATH_REPORT, ts_report_file)

    # 이메일 발송
    send_email_report(all_data, delta_results, extract_time, ts_data_file, ts_report_file)

    logger.info("통합 크롤링 및 아카이빙 완료!")

if __name__ == "__main__":
    main()
