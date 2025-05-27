# predictor.py
import pandas as pd
import numpy as np
import datetime
from crawler import crawl_hitter_data, crawl_pitcher_data, load_historical_data
from data_processor import process_hitter_data, process_pitcher_data


def generate_win_probability_df(all_hitter_data, all_pitcher_data):
    """팀별 승률 계산 및 OPS-WHIP 기반 팀 순위 예측"""
    # 연도가 2025인 데이터만 추출
    df_2025_hitter = all_hitter_data[all_hitter_data['연도'] == 2025]
    df_2025_pitcher = all_pitcher_data[all_pitcher_data['연도'] == 2025]

    # 팀별 OPS 값 평균 산출
    team_ops_avg = df_2025_hitter.groupby('팀명')['OPS_predict'].mean().reset_index()

    # OPS 기준 내림차순으로 팀 정렬 (이 부분은 승률 예측에는 사용되지 않지만, 디버깅 등에 유용)
    team_ops_avg_sorted = team_ops_avg.sort_values(by='OPS_predict', ascending=False)

    # 팀별 WHIP 값 평균 산출
    team_whip_avg = df_2025_pitcher.groupby('팀명')['WHIP_predict'].mean().reset_index()

    # WHIP 기준 내림차순으로 팀 정렬 (이 부분은 승률 예측에는 사용되지 않지만, 디버깅 등에 유용)
    team_whip_avg_sorted = team_whip_avg.sort_values(by='WHIP_predict', ascending=False)

    # 승률 계산 로직 (기존과 동일)
    teams = team_ops_avg['팀명'].tolist()
    win_probability_data = {}

    for team1 in teams:
        win_probability_data[team1] = {}
        for team2 in teams:
            if team1 == team2:
                win_probability_data[team1][team2] = '-'
            else:
                # 팀1의 OPS 예측값
                ops1 = team_ops_avg[team_ops_avg['팀명'] == team1]['OPS_predict'].iloc[0]
                # 팀2의 WHIP 예측값
                whip2 = team_whip_avg[team_whip_avg['팀명'] == team2]['WHIP_predict'].iloc[0]

                # 승률 예측 공식 (예시: OPS와 WHIP를 단순 합산하여 승률을 예측)
                # 실제 예측 모델은 더 복잡할 수 있습니다. 여기서는 예시로 간단한 공식을 사용합니다.
                # OPS는 높을수록 좋고, WHIP는 낮을수록 좋으므로, OPS1과 WHIP2의 역수를 사용
                # 예측 승률이 0보다 작거나 100보다 클 수 있으므로, 0-100% 범위로 스케일링 (예시)
                # 이 공식은 단순한 예시이며, 실제 야구 예측에 적합한 통계적 모델이 필요합니다.
                # 여기서는 OPS-WHIP을 활용하여 예측 승률을 계산하는 아이디어를 구현합니다.
                win_prob = (ops1 / (ops1 + whip2)) * 100 # 임의의 승률 계산식

                # 승률을 0% ~ 100% 사이로 제한
                win_prob = max(0, min(100, win_prob))

                win_probability_data[team1][team2] = round(win_prob, 2) # 소수점 둘째 자리까지 반올림

    win_probability_df = pd.DataFrame(win_probability_data)


    # OPS-WHIP 값 계산 및 팀 순위 예측 로직 추가
    # 팀별 OPS 및 WHIP 예측값을 하나의 DataFrame으로 합치기
    team_stats = pd.merge(team_ops_avg, team_whip_avg, on='팀명', suffixes=('_OPS', '_WHIP'))

    # OPS는 높을수록 좋고, WHIP는 낮을수록 좋으므로 'OPS_predict - WHIP_predict' 값을 계산합니다.
    # 이 값이 높을수록 팀의 공격력은 강하고 투수력은 좋다는 의미로 해석하여 순위를 예측합니다.
    team_stats['OPS_minus_WHIP'] = team_stats['OPS_predict_OPS'] - team_stats['WHIP_predict_WHIP']

    # OPS-WHIP 값을 기준으로 내림차순 정렬하여 최종 팀 순위 예측
    team_rankings = team_stats.sort_values(by='OPS_minus_WHIP', ascending=False).reset_index(drop=True)
    team_rankings['team_rank'] = team_rankings.index + 1 # 'rank' -> 'team_rank'으로 변경

    # 필요한 컬럼만 선택하여 반환 (팀명, 예측 OPS, 예측 WHIP, OPS-WHIP 값, 순위)
    predicted_team_rankings_df = team_rankings[['team_rank', '팀명', 'OPS_predict_OPS', 'WHIP_predict_WHIP', 'OPS_minus_WHIP']] # 'rank' -> 'team_rank'으로 변경
    predicted_team_rankings_df.columns = ['team_rank', 'team_name', 'predicted_ops', 'predicted_whip', 'ops_minus_whip'] # 'rank' -> 'team_rank'으로 변경

    return win_probability_df, predicted_team_rankings_df

def get_win_probability_df(cached_data):
    """캐시된 승률 DataFrame을 반환하거나 새로 생성"""
    current_time = datetime.datetime.now()

    # 렌더 배포 시 초기 데이터 로드를 위해, 만약 캐시가 비어있다면 강제로 새로고침
    if cached_data['hitter_data'] is None or cached_data['pitcher_data'] is None:
        print("Initial data load for Render deployment: Forcing data refresh.")
        cached_data['last_update'] = None # 캐시 업데이트 시간을 초기화하여 새로고침을 강제합니다.

    # 24시간(86,400초) 지났는지 확인
    if (cached_data['win_probability_df'] is None or
            cached_data['last_update'] is None or
            (current_time - cached_data['last_update']).total_seconds() > 86400):

        print("🔁 데이터 새로고침 시작...")

        # 크롤링 수행
        hitter_data_2025 = crawl_hitter_data()
        pitcher_data_2025 = crawl_pitcher_data()

        # 역대 데이터 로드
        hitter_data_his, pitcher_data_his = load_historical_data()

        # 데이터 처리
        all_hitter_data = process_hitter_data(hitter_data_2025, hitter_data_his)
        all_pitcher_data = process_pitcher_data(pitcher_data_2025, pitcher_data_his)

        # 승률 계산 및 팀 순위 예측
        win_probability_df, predicted_team_rankings_df = generate_win_probability_df(all_hitter_data, all_pitcher_data)

        # 캐시 업데이트
        cached_data['hitter_data'] = all_hitter_data
        cached_data['pitcher_data'] = all_pitcher_data
        cached_data['win_probability_df'] = win_probability_df
        cached_data['predicted_team_rankings_df'] = predicted_team_rankings_df # 추가된 예측 순위 저장
        cached_data['last_update'] = current_time
        print("✅ 데이터 새로고침 완료.")
    else:
        print("⏱️ 캐시된 데이터 사용 (업데이트 필요 없음).")

    return cached_data['win_probability_df']

def get_predicted_team_rankings_df(cached_data):
    """캐시된 팀 순위 예측 DataFrame을 반환"""
    # get_win_probability_df 함수가 호출될 때 이미 데이터가 업데이트되었으므로,
    # 여기서는 단순히 캐시된 데이터를 반환합니다.
    # 만약 get_win_probability_df가 호출되지 않은 상태에서 이 함수만 호출된다면
    # 데이터가 없을 수 있으므로, get_win_probability_df를 먼저 호출하도록 유도하거나
    # 여기서도 데이터 새로고침 로직을 포함할 수 있습니다.
    # 현재 설계에서는 run_daily_prediction_job에서 모두 처리되므로, 단순 반환합니다.
    if 'predicted_team_rankings_df' not in cached_data or cached_data['predicted_team_rankings_df'] is None:
        # 데이터가 캐시되지 않았을 경우, get_win_probability_df를 호출하여 데이터를 로드합니다.
        # 이 경우, win_probability_df와 함께 predicted_team_rankings_df도 업데이트됩니다.
        print("⏳ 팀 순위 예측 데이터가 캐시되지 않았습니다. 데이터를 새로고침합니다.")
        get_win_probability_df(cached_data) # 이 호출은 win_probability_df와 predicted_team_rankings_df를 모두 업데이트합니다.
    return cached_data['predicted_team_rankings_df']
