from flask import Flask, render_template, request, redirect, url_for, session, jsonify, flash
from flask_sqlalchemy import SQLAlchemy
from flask_session import Session
from datetime import datetime, timedelta
import requests
import json
import os
from urllib.parse import urlencode
import secrets

app = Flask(__name__)
app.config['SECRET_KEY'] = secrets.token_hex(32)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///upstox_app.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SESSION_TYPE'] = 'filesystem'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=24)

db = SQLAlchemy(app)
Session(app)

class ApiCredentials(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    api_key = db.Column(db.String(200), nullable=False)
    api_secret = db.Column(db.String(200), nullable=False)
    redirect_url = db.Column(db.String(500), default='http://127.0.0.1:5000/upstox/callback')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)

class UserSession(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    access_token = db.Column(db.Text, nullable=False)
    user_id = db.Column(db.String(100))
    user_name = db.Column(db.String(200))
    user_type = db.Column(db.String(50))
    email = db.Column(db.String(200))
    broker = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    expires_at = db.Column(db.DateTime)

with app.app_context():
    db.create_all()

@app.route('/')
def index():
    credentials = ApiCredentials.query.filter_by(is_active=True).first()
    user_session = UserSession.query.order_by(UserSession.created_at.desc()).first()
    return render_template('index.html', credentials=credentials, user_session=user_session)

@app.route('/settings', methods=['GET', 'POST'])
def settings():
    if request.method == 'POST':
        api_key = request.form.get('api_key')
        api_secret = request.form.get('api_secret')
        redirect_url = request.form.get('redirect_url', 'http://127.0.0.1:5000/upstox/callback')

        # Check if credentials exist
        credentials = ApiCredentials.query.filter_by(is_active=True).first()

        if credentials:
            credentials.api_key = api_key
            credentials.api_secret = api_secret
            credentials.redirect_url = redirect_url
            credentials.updated_at = datetime.utcnow()
        else:
            credentials = ApiCredentials(
                api_key=api_key,
                api_secret=api_secret,
                redirect_url=redirect_url
            )
            db.session.add(credentials)

        db.session.commit()
        flash('API Credentials saved successfully!', 'success')
        return redirect(url_for('index'))

    credentials = ApiCredentials.query.filter_by(is_active=True).first()
    return render_template('settings.html', credentials=credentials)

@app.route('/login')
def login():
    credentials = ApiCredentials.query.filter_by(is_active=True).first()

    if not credentials:
        flash('Please configure API credentials first!', 'error')
        return redirect(url_for('settings'))

    auth_params = {
        'client_id': credentials.api_key,
        'redirect_uri': credentials.redirect_url,
        'response_type': 'code'
    }

    auth_url = f"https://api.upstox.com/v2/login/authorization/dialog?{urlencode(auth_params)}"
    return redirect(auth_url)

@app.route('/upstox/callback')
def upstox_callback():
    code = request.args.get('code')

    if not code:
        flash('Authorization failed! No code received.', 'error')
        return redirect(url_for('index'))

    credentials = ApiCredentials.query.filter_by(is_active=True).first()

    if not credentials:
        flash('API credentials not found!', 'error')
        return redirect(url_for('settings'))

    # Exchange code for access token
    token_url = 'https://api.upstox.com/v2/login/authorization/token'

    token_data = {
        'code': code,
        'client_id': credentials.api_key,
        'client_secret': credentials.api_secret,
        'redirect_uri': credentials.redirect_url,
        'grant_type': 'authorization_code'
    }

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
        'Api-Version': '2.0'
    }

    try:
        response = requests.post(token_url, data=token_data, headers=headers)

        # Debug logging
        print(f"Token exchange response status: {response.status_code}")
        print(f"Token exchange response: {response.text}")

        response.raise_for_status()

        token_response = response.json()

        # Upstox returns the user data directly with access_token
        access_token = token_response.get('access_token')

        if access_token:
            # The token response already contains user info
            user_info = token_response

            user_session = UserSession(
                access_token=access_token,
                user_id=user_info.get('user_id'),
                user_name=user_info.get('user_name'),
                user_type=user_info.get('user_type'),
                email=user_info.get('email'),
                broker=user_info.get('broker')
            )

            db.session.add(user_session)
            db.session.commit()

            session['access_token'] = access_token
            session['user_id'] = user_info.get('user_id')
            session['user_name'] = user_info.get('user_name')

            flash(f'Welcome {user_info.get("user_name", "User")}! Login successful.', 'success')
            return redirect(url_for('dashboard'))
        else:
            error_msg = 'Failed to get access token from response'
            flash(f'Authentication failed: {error_msg}', 'error')
            print(f"Token exchange failed - no access token in response: {token_response}")
            return redirect(url_for('index'))

    except requests.exceptions.RequestException as e:
        flash(f'Network error during authentication: {str(e)}', 'error')
        print(f"Request exception: {str(e)}")
        return redirect(url_for('index'))
    except Exception as e:
        flash(f'Error during authentication: {str(e)}', 'error')
        print(f"General exception: {str(e)}")
        return redirect(url_for('index'))

@app.route('/dashboard')
def dashboard():
    if 'access_token' not in session:
        flash('Please login first!', 'warning')
        return redirect(url_for('index'))

    return render_template('dashboard.html', user_name=session.get('user_name'))

@app.route('/api/funds')
def get_funds():
    if 'access_token' not in session:
        return jsonify({'error': 'Unauthorized'}), 401

    funds_url = 'https://api.upstox.com/v2/user/get-funds-and-margin'
    headers = {
        'Authorization': f'Bearer {session["access_token"]}',
        'Accept': 'application/json'
    }

    try:
        response = requests.get(funds_url, headers=headers)
        response.raise_for_status()
        return jsonify(response.json())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/logout')
def logout():
    session.clear()
    flash('Logged out successfully!', 'info')
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True, port=5000)