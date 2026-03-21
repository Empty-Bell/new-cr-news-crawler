import pandas as pd

try:
    cols1 = ["게재 일자", "기사 제목", "URL 링크", "Supercategory", "Category", "언급 브랜드", "내용 요약", "핵심 인사이트", "보고용 멘트", "중요도"]
    cols2 = ["게재 일자", "기사 제목", "URL 링크"]
    
    df1 = pd.DataFrame(columns=cols1)
    df2 = pd.DataFrame(columns=cols2)
    
    with pd.ExcelWriter("CR_News_Report_Master.xlsx", engine="openpyxl") as w:
        df1.to_excel(w, sheet_name="Target_Articles", index=False)
        df2.to_excel(w, sheet_name="All_Articles_History", index=False)
    print("Success")
except Exception as e:
    print(e)
