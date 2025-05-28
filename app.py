# app.py
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
import predictor # predictor 모듈에서 get_predicted_team_rankings_df 함수를 사용하기 위해 임포트
from tasks import run_daily_prediction_job
import os # os 모듈을 임포트해야 합니다.
import datetime # datetime 모듈 임포트 추가

# .env 파일에서 환경 변수 로드 (이 라인은 이제 DB_URI에는 영향을 미치지 않지만, 다른 환경 변수 로드에 필요할 수 있으므로 유지)
load_dotenv()

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

# MySQL 연결 정보 직접 명시
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv(
'DB_URI',
'mysql+pymysql://root:dugout2025!!@dugout-dev.cn6mm486utfi.ap-northeast-2.rds.amazonaws.com:3306/dugoutDB?charset=utf8'
)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False


# 전역 변수로 데이터와 모델 저장 (API 호출 시 사용)
cached_data = {
    'hitter_data': None,
    'pitcher_data': None,
    'win_probability_df': None,
    'predicted_team_rankings_df': None, # 팀 순위 예측 결과를 저장할 캐시 추가
    'last_update': None
}

# --- 이 부분이 중요합니다! 스케줄러와 초기 실행 로직을 __name__ == '__main__' 밖으로 이동 ---\

# 스케줄러 설정
scheduler = BackgroundScheduler(daemon=True, timezone='Asia/Seoul')
# 매일 00:01에 run_daily_prediction_job 함수 실행
scheduler.add_job(run_daily_prediction_job, 'cron', hour=10, minute=1)
scheduler.start()

# 애플리케이션 시작 시 최초 1회 실행
with app.app_context():
    print("🚀 애플리케이션 시작: 초기 데이터 로드 및 예측 작업 실행...")
    run_daily_prediction_job() # 앱 시작 시 초기 데이터 로드 및 DB 적재
    print("✅ 초기 데이터 로드 및 예측 작업 완료.")


@app.route('/predict_win_probability', methods=['POST'])
def predict_win_probability():
    data = request.get_json()
    if not data or 'team1' not in data or 'team2' not in data:
        return jsonify({'error': '두 팀 이름을 제공해야 합니다. 예: {"team1": "LG", "team2": "삼성"}'}), 400

    team1 = data['team1']
    team2 = data['team2']

    try:
        # API 요청 시에는 캐시 기반으로 예측 결과 제공
        # 이 호출을 통해 내부적으로 캐시된 데이터가 최신으로 유지됩니다.
        win_probability_df = predictor.get_win_probability_df(cached_data)

        valid_teams = win_probability_df.index.tolist()

        if team1 not in valid_teams:
            return jsonify({'error': f"'{team1}'은(는) 유효한 팀 이름이 아닙니다. 유효한 팀 목록: {', '.join(valid_teams)}", 'valid_teams': valid_teams}), 400
        if team2 not in valid_teams:
            return jsonify({'error': f"'{team2}'은(는) 유효한 팀 이름이 아닙니다. 유효한 팀 목록: {', '.join(valid_teams)}", 'valid_teams': valid_teams}), 400
        if team1 == team2:
            return jsonify({'error': "같은 팀 간의 승률은 계산할 수 없습니다."}), 400

        win_prob = win_probability_df.loc[team1, team2]
        if win_prob == '-':
            return jsonify({'error': "승률을 계산할 수 없습니다."}), 400

        return jsonify({
            'team1': team1,
            'team2': team2,
            'win_probability': float(win_prob),
            'message': f"{team1}이(가) {team2}을(를) 상대로 승리할 예측 승률은 {win_prob}% 입니다."
        })

    except Exception as e:
        return jsonify({'error': f"승률 예측 중 오류가 발생했습니다: {str(e)}"}), 500

@app.route('/predict_team_rankings', methods=['GET'])
def predict_team_rankings():
    """예측된 팀 순위를 반환하는 API 엔드포인트"""
    try:
        predicted_rankings_df = predictor.get_predicted_team_rankings_df(cached_data)
        if predicted_rankings_df is None:
            return jsonify({'error': '팀 순위 예측 데이터를 불러올 수 없습니다. 데이터 로딩 중이거나 오류가 발생했습니다.'}), 500

        # DataFrame을 JSON 형식으로 변환하여 반환
        return jsonify(predicted_rankings_df.to_dict(orient='records'))

    except Exception as e:
        return jsonify({'error': f"팀 순위 예측 결과를 가져오는 중 오류가 발생했습니다: {str(e)}"}), 500

@app.route('/')
def home():
    return "환영합니다! KBO 승률 예측 API입니다. /predict_win_probability (POST) 또는 /predict_team_rankings (GET) 엔드포인트를 사용하세요."

if __name__ == '__main__':
    # Render 환경에서는 Gunicorn과 같은 WSGI 서버가 앱을 실행하므로
    # 이 __main__ 블록의 app.run()은 직접 실행되지 않습니다.
    app.run(host='0.0.0.0', port=os.getenv('PORT', 5000), debug=True)
