import os
import time
import json
import logging
import re
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

# Use existing selenium driver setup from the unified crawler
from cr_crawler_unified import setup_driver, auto_login

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
    summary_text = f"총 {total_news_count}건의 기사가 게재되었으나 타겟 기사는 없었습니다." if new_articles_count == 0 else f"총 {total_news_count}건(전체 기사) 중 {new_articles_count}건(타겟 기사)"
    
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
            <li>수집 대상: {summary_text}</li>
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
    # 데일리 실행 시 오늘 날짜 기사만 수집 (마스터 파일 중복 방지)
    current_target_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
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
    max_pages = 5 # 데일리는 상위 페이지만 확인해도 충분
    stop_scraping = False
    total_news_count = 0
    
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
            cards = driver.find_elements(By.CSS_SELECTOR, "div.news-list a, .crux-article-card")
            if not cards: break
            
            total_news_count += len(cards)
            old_article_count = 0
            
            for card in cards:
                try:
                    link = card.get_attribute("href")
                    if not link or link in seen_links or link in existing_links: continue
                    
                    try:
                        title = card.find_element(By.TAG_NAME, "h3").text.strip()
                    except: title = "Untitled Article"
                    
                    date_raw = ""
                    for ds in [".news-item__timestamp", "p.crux-body-copy--extra-small"]:
                        try:
                            date_raw = card.find_element(By.CSS_SELECTOR, ds).text.strip()
                            if date_raw: break
                        except: continue
                    
                    pub_date = get_parsed_date(date_raw)
                    if pub_date and pub_date < current_target_date:
                        old_article_count += 1
                        continue
                    
                    # 타겟 필터
                    TARGET_URL_PATHS = ['/appliances/', '/electronics/', '/electronics-computers/', '/home-garden/']
                    WHITELIST_KEYWORDS = ['washer', 'dryer', 'refrigerator', 'dishwasher', 'vacuum', 'oven', 'range', 'cooktop', 'microwave', 'air purifier', 'tv', 'monitor', 'laptop', 'tablet', 'smartphone', 'cleaning', 'appliance', 'electronics']
                    
                    link_lower = link.lower()
                    title_lower = title.lower()
                    if any(tp in link_lower for tp in TARGET_URL_PATHS) or any(kw in title_lower for kw in WHITELIST_KEYWORDS):
                        logger.info(f"  [+] 신규 타겟 발견: {title}")
                        article_jobs.append({"title": title, "link": link, "pub_date": pub_date, "date_raw": date_raw})
                        seen_links.add(link)
                except: continue
            
            if old_article_count >= 8: break 
            page_num += 1
        except: break

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
