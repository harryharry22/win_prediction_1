# db_utils.py
import os
import pandas as pd
from sqlalchemy import create_engine, text # text 모듈 임포트 추가
from dotenv import load_dotenv
import datetime

def get_db_engine():
    """DB 연결을 위한 SQLAlchemy 엔진 생성"""
    #DB URI를 직접 명시
    db_uri = 'mysql+pymysql://root:dugout2025!!@dugout-dev.cn6mm486utfi.ap-northeast-2.rds.amazonaws.com:3306/dugoutDB?charset=utf8'

    # 디버깅을 위해 print 추가
    print(f"DEBUG: Using DB URI: {db_uri.split('//')[0]}//****:****@{db_uri.split('@')[1]}") # 비밀번호는 가립니다.

    if not db_uri:
        raise ValueError("DB URI가 올바르게 설정되지 않았습니다.")

    engine = create_engine(db_uri)
    return engine

def save_win_probabilities(win_probability_df):
    """승률 예측 DataFrame을 DB에 저장 (기존 데이터 삭제 후)"""
    try:
        engine = get_db_engine()
        
        # 1. 기존 데이터 삭제
        table_name = 'win_probabilities'
        with engine.connect() as connection:
            connection.execute(text(f"TRUNCATE TABLE {table_name};")) # TRUNCATE TABLE 사용
            connection.commit() # 변경사항 커밋
        print(f"✅ 기존 '{table_name}' 테이블의 모든 데이터가 삭제되었습니다.")

        # DataFrame을 DB에 저장하기 좋은 긴 형식(long format)으로 변환
        long_format_df = win_probability_df.stack().reset_index()
        long_format_df.columns = ['team1', 'team2', 'win_probability']

        # 'team1'과 'team2'가 같지 않은 경우만 필터링 (자기 자신과의 승률은 불필요)
        long_format_df = long_format_df[long_format_df['team1'] != long_format_df['team2']].copy()

        # 데이터 타입 변환 및 예측 날짜 추가
        long_format_df['win_probability'] = pd.to_numeric(long_format_df['win_probability'])
        long_format_df['prediction_date'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 2. 새로운 데이터 저장
        # if_exists='append'를 사용하지만, 위에 TRUNCATE를 했으므로 사실상 'replace'와 유사하게 동작
        long_format_df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"✅ 승률 예측 결과 {len(long_format_df)}건이 '{table_name}' 테이블에 성공적으로 저장되었습니다.")
    except Exception as e:
        print(f"❌ 승률 예측 결과 DB 저장 중 오류 발생: {e}")

def save_team_rankings(team_rankings_df):
    """팀 순위 예측 DataFrame을 DB에 저장 (기존 데이터 삭제 후)"""
    try:
        engine = get_db_engine()

        # 1. 기존 데이터 삭제
        table_name = 'team_rankings'
        with engine.connect() as connection:
            connection.execute(text(f"TRUNCATE TABLE {table_name};")) # TRUNCATE TABLE 사용
            connection.commit() # 변경사항 커밋
        print(f"✅ 기존 '{table_name}' 테이블의 모든 데이터가 삭제되었습니다.")

        # 예측 날짜 추가
        team_rankings_df['prediction_date'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 2. 새로운 데이터 저장
        team_rankings_df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"✅ 팀 순위 예측 결과 {len(team_rankings_df)}건이 '{table_name}' 테이블에 성공적으로 저장되었습니다.")
    except Exception as e:
        print(f"❌ 팀 순위 예측 결과 DB 저장 중 오류 발생: {e}")

def save_hitter_data(hitter_df):
    """크롤링된 타자 데이터를 DB에 저장 (기존 데이터 삭제 후)"""
    try:
        engine = get_db_engine()
        table_name = 'hitter_data' # 타자 데이터를 저장할 테이블 이름

        # 1. 기존 데이터 삭제
        with engine.connect() as connection:
            connection.execute(text(f"TRUNCATE TABLE {table_name};"))
            connection.commit()
        print(f"✅ 기존 '{table_name}' 테이블의 모든 데이터가 삭제되었습니다.")

        # 2. 새로운 데이터 저장
        # 컬럼 이름을 DB에 맞게 조정 (예: '순위' -> 'rank', '선수명' -> 'player_name' 등)
        # 현재 코드에서는 크롤링된 DataFrame의 컬럼명 그대로 사용합니다.
        # 필요하다면 여기서 컬럼명을 변경하는 로직을 추가할 수 있습니다.
        hitter_df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"✅ 타자 데이터 {len(hitter_df)}건이 '{table_name}' 테이블에 성공적으로 저장되었습니다.")
    except Exception as e:
        print(f"❌ 타자 데이터 DB 저장 중 오류 발생: {e}")

def save_pitcher_data(pitcher_df):
    """크롤링된 투수 데이터를 DB에 저장 (기존 데이터 삭제 후)"""
    try:
        engine = get_db_engine()
        table_name = 'pitcher_data' # 투수 데이터를 저장할 테이블 이름

        # 1. 기존 데이터 삭제
        with engine.connect() as connection:
            connection.execute(text(f"TRUNCATE TABLE {table_name};"))
            connection.commit()
        print(f"✅ 기존 '{table_name}' 테이블의 모든 데이터가 삭제되었습니다.")

        # 2. 새로운 데이터 저장
        # 컬럼 이름을 DB에 맞게 조정
        pitcher_df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"✅ 투수 데이터 {len(pitcher_df)}건이 '{table_name}' 테이블에 성공적으로 저장되었습니다.")
    except Exception as e:
        print(f"❌ 투수 데이터 DB 저장 중 오류 발생: {e}")
