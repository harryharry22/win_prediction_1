# app.py
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
import predictor
from tasks import run_daily_prediction_job
import os # os 모듈을 임포트해야 합니다.

# .env 파일에서 환경 변수 로드 (이 라인은 이제 DB_URI에는 영향을 미치지 않지만, 다른 환경 변수 로드에 필요할 수 있으므로 유지)
load_dotenv()

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

# MySQL 연결 정보 직접 명시 (환경 변수 사용 권장하지 않음)
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
    'last_update': None
}

@app.route('/')
def home():
    return "KBO 야구 승률 예측 API. '/predict_win_rate' 엔드포인트를 사용하세요."

@app.route('/predict_win_rate', methods=['POST'])
def predict_win_rate():
    # 요청에서 팀 이름 추출
    data = request.get_json()
    if not data or 'team1' not in data or 'team2' not in data:
        return jsonify({'error': '두 팀 이름을 제공해야 합니다. 예: {"team1": "LG", "team2": "삼성"}'}), 400

    team1 = data['team1']
    team2 = data['team2']

    try:
        # API 요청 시에는 캐시 기반으로 예측 결과 제공
        win_probability_df = predictor.get_win_probability_df(cached_data)

        valid_teams = win_probability_df.index.tolist()

        if team1 not in valid_teams:
            return jsonify({'error': f"'{team1}'은(는) 유효한 팀 이름이 아닙니다. 유효한 팀 목록: {', '.join(valid_teams)}"}), 400
        if team2 not in valid_teams:
            return jsonify({'error': f"'{team2}'은(는) 유효한 팀 이름이 아닙니다. 유효한 팀 목록: {', '.join(valid_teams)}"}), 400
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
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # 스케줄러 설정
    scheduler = BackgroundScheduler(daemon=True, timezone='Asia/Seoul')
    # 매일 00:01에 run_daily_prediction_job 함수 실행
    scheduler.add_job(run_daily_prediction_job, 'cron', hour=0, minute=1)
    scheduler.start()
    
    # 앱을 처음 시작할 때 작업을 한 번 실행하여 DB를 채울 수 있습니다.
    run_daily_prediction_job() 
    
    print("🚀 API 서버와 스케줄러가 시작되었습니다. 매일 00:01에 예측 결과가 DB에 저장됩니다.")
    app.run(debug=True, host='0.0.0.0', port=8080)
