import os
import time
import json
import logging
import random
import re
from datetime import datetime, timedelta, timezone
import pandas as pd
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
import google.generativeai as genai

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Gemini
api_key = os.getenv("GEMINI_API_KEY")
if api_key:
    genai.configure(api_key=api_key)
else:
    logger.warning("GEMINI_API_KEY is not set.")

MASTER_FILE = "CR_News_Report_Master.xlsx"

def normalize_url(url):
    """URL의 대소문자나 끝의 슬래시 유무 등을 통일합니다."""
    if not url: return ""
    return url.strip().lower().split('#')[0].split('?')[0].rstrip('/')

# KST 시간대 설정
KST = timezone(timedelta(hours=9))

def kst_converter(*args):
    return datetime.now(KST).timetuple()

logging.Formatter.converter = kst_converter

# ============================================================
# 유틸리티 함수 (드라이버 설정 및 로그인)
# ============================================================
def setup_driver(profile_path):
    chrome_options = Options()
    if os.getenv("GITHUB_ACTIONS") == "true":
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        logger.info("클라우드 환경 감지: 헤드리스 모드로 실행합니다.")
    
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

def human_type(element, text):
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(0.1, 0.3))

def auto_login(driver):
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
        username_field = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#username")))
        password_field = driver.find_element(By.CSS_SELECTOR, "#password")
        login_button = driver.find_element(By.CSS_SELECTOR, "button.qa-sign-in-button")
        
        human_type(username_field, email)
        time.sleep(random.uniform(0.5, 1.2))
        human_type(password_field, password)
        time.sleep(random.uniform(0.5, 1.2))
        login_button.click()
        time.sleep(8)
        
        login_success = False
        try:
            sign_in_elements = driver.find_elements(By.CSS_SELECTOR, "label#sign-in-label, .qa-sign-in-button")
            if not sign_in_elements or not any(el.is_displayed() for el in sign_in_elements):
                login_success = True
        except: pass

        if not login_success:
            member_elements = driver.find_elements(By.CSS_SELECTOR, ".cda-gnav__member--shown, .cda-gnav__account-menu")
            if any(el.is_displayed() for el in member_elements):
                login_success = True

        if not login_success:
            if "login" not in driver.current_url.lower():
                login_success = True

        if login_success:
            logger.info("자동 로그인 성공 확인 완료!")
            return True
        else:
            logger.warning(f"로그인 성공 여부를 확신할 수 없습니다. (URL: {driver.current_url})")
            return False
    except Exception as e:
        logger.error(f"자동 로그인 도중 에러 발생: {e}")
        return False

def analyze_article_with_llm(title, content):
    """
    LLM을 사용하여 기사 내용을 분석하고, 필터링 및 중요도를 평가합니다.
    """
    system_prompt = """
    You are an expert analyst for Samsung Electronics.
    Analyze the given Consumer Reports news article and extract information based on the following classification logic in JSON format ONLY:
    
    1. Filter & Priority Algorithm:
       - 1st Filter (Targeting): 
         Whitelist keywords: Home Appliances (refrigerator, washer, dryer, dishwasher, microwave, vacuum, cooktop, wall-oven, range, air conditioner, TV, monitor, smartphone), electronics, computing.
         Blacklist keywords: Cars, Tires, Baby items, Laundry Detergents, Food, Insurance, Finance. -> If blacklist is primary topic, "is_target" should be false.
       
       - 2nd Filter (Scoring):
         Look for 'Samsung' or 'LG'. Pay high attention to product rankings and "Recommended" status.
       
       - 3rd Filter (Classification/Importance):
         [High]: LG 등 경쟁사가 주요 카테고리의 추천 제품(Top Pick)을 전부 휩쓸었거나, 브랜드 신뢰도 등급에 대한 새로운/중요 발표가 포함된 경우.
         [Medium]: 당사(Samsung) 모델이 Top Pick으로 선정되었거나, 경쟁사가 Top Pick에 포함되었으나 전 부문을 휩쓰는 정도는 아닌 경우.
         [Low]: 당사 및 경쟁사 관련 특정 이슈 없이 일반적인 제품 소개나 할인, 부속품 등에 대한 내용일 경우.

    2. Output JSON Schema (Must be strictly valid JSON):
       {
           "is_target": boolean,
           "supercategory": "String",
           "category": "String",
           "brands_mentioned": "String",
           "summary": "String", 
           "core_insight": "String",
           "actionable_comment": "String",
           "importance": "String"
       }
    """
    
    user_prompt = f"Title: {title}\n\nContent:\n{content[:25000]}"
    full_prompt = f"{system_prompt}\n\n{user_prompt}"
    
    try:
        model = genai.GenerativeModel('gemini-flash-latest')
        for attempt in range(3):
            try:
                response = model.generate_content(
                    full_prompt,
                    generation_config=genai.types.GenerationConfig(
                        response_mime_type="application/json",
                        temperature=0.2
                    )
                )
                return json.loads(response.text)
            except Exception as e:
                if "429" in str(e) and attempt < 2:
                    logger.warning(f"Quota 초과 (429). 12초 후 재시도 ({attempt+1}/3)...")
                    time.sleep(12)
                    continue
                raise e
    except Exception as e:
        logger.error(f"LLM API 에러: {e}")
        return {"is_target": False, "importance": "Low"}

def get_parsed_date(date_str):
    """불필요한 텍스트를 제거하고 순수 날짜만 추출합니다."""
    match = re.search(r'([A-Z][a-z]+ \d{1,2}, \d{4})', date_str)
    if match:
        try:
            return pd.to_datetime(match.group(1)).to_pydatetime()
        except:
            return None
    return None

def send_email_news_report(new_articles_count, total_news_count, final_df):
    """지정된 양식에 맞춰 이메일을 발송합니다."""
    smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", 587))
    sender_email = os.getenv("SENDER_EMAIL", "your_email@gmail.com")
    sender_password = os.getenv("SENDER_PASSWORD", "your_password")
    receiver_email = "jongbin.yun@samsung.com"

    report_date = datetime.now().strftime("%Y-%m-%d")
    
    # 중요도 분포 계산 (오늘 날짜 기사 기준)
    today_articles = final_df[final_df['게재 일자'].astype(str) == report_date]
    importance_counts = today_articles['중요도'].value_counts()
    high_cnt = importance_counts.get("High", 0)
    mid_cnt = importance_counts.get("Medium", 0)
    low_cnt = importance_counts.get("Low", 0)

    # 메일 본문 구성
    summary_text = f"신규 게재된 기사가 없었습니다." if total_news_count == 0 else (f"총 {total_news_count}건의 신규 기사 중 타겟 기사는 없었습니다." if new_articles_count == 0 else f"총 {total_news_count}건(신규 기사) 중 {new_articles_count}건(타겟 기사)")
    
    dist_html = ""
    if new_articles_count > 0:
        dist_html = f"  - 중요도 분포: High ({high_cnt}건) / Medium ({mid_cnt}건) / Low ({low_cnt}건)"

    # 수집 기사 정리표 (HTML) - 최신 20건 발췌
    tbl_df = final_df.head(20).copy()
    tbl_html = tbl_df.to_html(index=False, border=1, justify='center', na_rep='')
    tbl_html = tbl_html.replace('<table', '<table style="border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; font-size: 11px;"')
    tbl_html = tbl_html.replace('<th', '<th style="background-color: #f2f2f2; border: 1px solid #ddd; padding: 8px;"')
    tbl_html = tbl_html.replace('<td', '<td style="border: 1px solid #ddd; padding: 6px; text-align: center;"')

    body = f"""
    <html>
    <body>
        <p>□ 일일 수집 요약</p>
        <ul>
            <li>보고 일자: {report_date}</li>
            <li>수집 결과: {summary_text}</li>
            {f'<li>{dist_html}</li>' if dist_html else ''}
        </ul>
        <br>
        <p>□ 수집 기사 정리표(누적 기준 최근 20건)</p>
        {tbl_html}
        <br>
        <p>※ 상세 내용은 첨부된 마스터 파일을 확인해 주세요.</p>
    </body>
    </html>
    """

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = f"[CR 일일 동향] Consumer Reports 주요 뉴스 브리핑 ({report_date})"
    msg.attach(MIMEText(body, 'html'))

    if os.path.exists(MASTER_FILE):
        with open(MASTER_FILE, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename= {MASTER_FILE}")
        msg.attach(part)

    try:
        # SMTP 설정이 되어 있는 경우에만 발송
        if sender_email != "your_email@gmail.com" and sender_password != "your_password":
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(sender_email, sender_password)
                server.send_message(msg)
            logger.info(f"✅ 이메일 발송 완료: {receiver_email}")
        else:
            logger.warning("이메일 계정 설정(SMTP)이 필요합니다. 리포트 발송을 건너뜁니다.")
    except Exception as e:
        logger.error(f"❌ 이메일 발송 실패: {e}")

def main():
    # 마지막 수집 시점의 최상단 기사 URL 확인 (신규 기사 건수 산정용)
    HISTORY_FILE = "last_top_url.txt"
    last_top_url = ""
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r") as f:
                last_top_url = f.read().strip()
            logger.info(f"Last Top Article URL: {last_top_url}")
        except: pass
    
    # 기존 데이터 로드 (마스터 파일 누적 관리)
    if os.path.exists(MASTER_FILE):
        try:
            master_df = pd.read_excel(MASTER_FILE)
            existing_links = set(master_df['URL 링크'].astype(str).tolist())
        except Exception as e:
            logger.error(f"마스터 파일 로드 오류: {e}")
            master_df = pd.DataFrame()
            existing_links = set()
    else:
        master_df = pd.DataFrame()
        existing_links = set()

    import tempfile
    profile_dir = tempfile.mkdtemp()
    logger.info(f"Session Profile: {profile_dir}")
    driver = setup_driver(profile_dir)
    
    auto_login(driver)
    
    new_data = [] # 오늘 수집된 데이터
    article_jobs = [] # 분석 대기 기사
    seen_links = set()
    page_num = 1
    max_pages = 5
    stop_scraping = False
    total_news_count = 0
    new_top_url = None
    
    # (1) 기사 목록 탐색
    while page_num <= max_pages and not stop_scraping:
        url = f"https://www.consumerreports.org/cro/news/index.htm#page={page_num}"
        logger.info(f"뉴스 인덱스 {page_num}페이지 진입")
        driver.get(url)
        if page_num > 1:
            time.sleep(2)
            driver.refresh()
        
        time.sleep(5)
        try:
            # 기사 카드 선택 (중복 제거를 위해 unique한 앵커 태그 추출)
            all_links = driver.find_elements(By.CSS_SELECTOR, "div.news-list a, .crux-article-card")
            
            cards_data = []
            seen_page_links = set()
            for al in all_links:
                try:
                    l = al.get_attribute("href")
                    if l and "/news/" not in l and l not in seen_page_links and "consumerreports.org" in l:
                        # h3가 있는 a태그나 article card만 유효한 기사로 간주
                        try:
                            t = al.find_element(By.TAG_NAME, "h3").text.strip()
                        except:
                            # 텍스트가 없으면 기사 목록이 아닐 수 있음
                            continue
                        
                        cards_data.append({"link": l, "title": t, "element": al})
                        seen_page_links.add(l)
                except: continue
            
            if not cards_data: break
            
            for item in cards_data:
                link = item["link"]
                title = item["title"]
                card = item["element"]
                
                # 이번 실행의 최상단 기사 URL 저장 (첫 페이지 첫 번째 항목)
                if page_num == 1 and new_top_url is None:
                    new_top_url = link
                
                # 이전 수집 시점의 최상단 기사를 만난 경우 중단 (URL 정규화 비교)
                if normalize_url(link) == normalize_url(last_top_url):
                    logger.info(f"  [!] 이전 수집 지점({last_top_url})에 도달했습니다. 중단합니다.")
                    stop_scraping = True
                    break
                
                total_news_count += 1
                
                # 이미 수집된 타겟 기사인 경우 건너뛰기
                if link in existing_links or link in seen_links:
                    continue
                
                # 기사 날짜 추출
                date_raw = ""
                for ds in [".news-item__timestamp", "p.crux-body-copy--extra-small"]:
                    try:
                        date_raw = card.find_element(By.CSS_SELECTOR, ds).text.strip()
                        if date_raw: break
                    except: continue
                
                pub_date = get_parsed_date(date_raw)
                
                # 타겟 필터
                TARGET_URL_PATHS = ['/appliances/', '/electronics/', '/electronics-computers/', '/home-garden/']
                WHITELIST_KEYWORDS = ['washer', 'dryer', 'refrigerator', 'dishwasher', 'vacuum', 'oven', 'range', 'cooktop', 'microwave', 'air purifier', 'tv', 'monitor', 'laptop', 'tablet', 'smartphone', 'cleaning', 'appliance', 'electronics']
                
                link_lower = link.lower()
                title_lower = title.strip().lower()
                
                is_whitelist = any(tp in link_lower for tp in TARGET_URL_PATHS) or any(kw in title_lower for kw in WHITELIST_KEYWORDS)
                
                if is_whitelist:
                    logger.info(f"  [+] 신규 타겟 발견: {title}")
                    article_jobs.append({"title": title, "link": link, "pub_date": pub_date, "date_raw": date_raw})
                    seen_links.add(link)
            
            page_num += 1
        except Exception as e:
            logger.error(f"목록 수집 중 오류: {e}")
            break

    # 최상단 기사 URL 업데이트 (다음 실행용)
    if new_top_url:
        try:
            with open(HISTORY_FILE, "w") as f:
                f.write(new_top_url)
            logger.info(f"Updated Last Top Article URL: {new_top_url}")
        except: pass

    # (2) 기사 본문 분석
    for i, job in enumerate(article_jobs):
        try:
            logger.info(f"[{i+1}/{len(article_jobs)}] 분석 중: {job['title']}")
            driver.get(job['link'])
            time.sleep(6)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(2)
            
            content_builder = [f"Title: {job['title']}"]
            seen_text = set()
            for sel in [".cda-article__main-content", "article"]:
                try:
                    container = driver.find_element(By.CSS_SELECTOR, sel)
                    for p in container.find_elements(By.TAG_NAME, "p"):
                        txt = p.text.strip()
                        if txt and len(txt) > 30 and txt not in seen_text:
                            content_builder.append(txt)
                            seen_text.add(txt)
                except: continue
            
            for rs in [".rating-component", ".recent-recommended-model"]:
                try:
                    for r in driver.find_elements(By.CSS_SELECTOR, rs):
                        content_builder.append(f"Ranking: {r.text.strip()}")
                except: continue

            content = "\n".join(content_builder)
            if len(content) > 300:
                analysis = analyze_article_with_llm(job['title'], content)
                if analysis.get("is_target"):
                    new_data.append({
                        "게재 일자": job['pub_date'].strftime("%Y-%m-%d") if job['pub_date'] else job['date_raw'],
                        "기사 제목": job['title'], "URL 링크": job['link'],
                        "Supercategory": analysis.get("supercategory", ""), "Category": analysis.get("category", ""),
                        "언급 브랜드": analysis.get("brands_mentioned", ""), "내용 요약": analysis.get("summary", ""),
                        "핵심 인사이트": analysis.get("core_insight", ""), "보고용 멘트": analysis.get("actionable_comment", ""),
                        "중요도": analysis.get("importance", "")
                    })
        except: continue

    # (3) 데이터 병합 및 최신순 정렬
    if new_data:
        new_df = pd.DataFrame(new_data)
        final_df = pd.concat([new_df, master_df], ignore_index=True)
    else:
        final_df = master_df
    
    if not final_df.empty:
        final_df = final_df.drop_duplicates(subset=['URL 링크'], keep='first')
        final_df['temp_date'] = pd.to_datetime(final_df['게재 일자'], errors='coerce')
        final_df = final_df.sort_values(by='temp_date', ascending=False).drop(columns=['temp_date'])
        final_df.to_excel(MASTER_FILE, index=False)
        
        # (4) 최종 이메일 발송
        send_email_news_report(len(new_data), total_news_count, final_df)

    driver.quit()

if __name__ == "__main__":
    main()
