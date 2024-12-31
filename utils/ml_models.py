"""
Machine Learning Model Utilities
"""
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier, IsolationForest
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import streamlit as st

def train_load_forecasting_model(data, features, target='total_consumption_kwh'):
    """Train load forecasting model"""
    X = data[features]
    y = data[target]
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)
    
    train_score = model.score(X_train, y_train)
    test_score = model.score(X_test, y_test)
    
    return model, train_score, test_score, X_test, y_test

def detect_anomalies(data, features, contamination=0.1):
    """Detect anomalies using Isolation Forest"""
    X = data[features].fillna(0)
    
    model = IsolationForest(contamination=contamination, random_state=42, n_jobs=-1)
    predictions = model.fit_predict(X)
    
    # -1 for anomalies, 1 for normal
    data['is_anomaly'] = predictions == -1
    data['anomaly_score'] = model.score_samples(X)
    
    return data, model

def segment_consumers(data, features, n_clusters=5):
    """Segment consumers using K-Means clustering"""
    X = data[features].fillna(0)
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    data['segment'] = kmeans.fit_predict(X_scaled)
    
    return data, kmeans, scaler

def predict_churn(data, features, target='is_churned'):
    """Predict customer churn"""
    X = data[features].fillna(0)
    y = data[target]
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    model = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)
    
    train_score = model.score(X_train, y_train)
    test_score = model.score(X_test, y_test)
    
    # Feature importance
    feature_importance = pd.DataFrame({
        'feature': features,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    return model, train_score, test_score, feature_importance
