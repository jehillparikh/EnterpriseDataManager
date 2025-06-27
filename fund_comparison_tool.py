"""
Fund Comparison Tool for Mutual Funds
Compares multiple funds across various metrics and parameters
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from collections import defaultdict

from setup_db import db
from models import (
    Fund, FundFactSheet, FundReturns, FundHolding, NavHistory,
    FundRating, FundAnalytics, FundStatistics, FundCodeLookup
)

logger = logging.getLogger(__name__)

class FundComparisonTool:
    """
    Comprehensive fund comparison tool with multiple comparison metrics
    """
    
    def __init__(self):
        """Initialize the fund comparison tool"""
        pass
    
    def compare_funds(self, fund_isins: List[str], comparison_type: str = "comprehensive") -> Dict:
        """
        Compare multiple funds across various metrics
        
        Args:
            fund_isins (List[str]): List of fund ISINs to compare
            comparison_type (str): Type of comparison - 'comprehensive', 'performance', 'risk', 'cost'
            
        Returns:
            Dict: Comprehensive comparison analysis
        """
        logger.info(f"Comparing {len(fund_isins)} funds with {comparison_type} analysis")
        
        if len(fund_isins) < 2:
            return {"error": "At least 2 funds required for comparison"}
        
        # Validate funds and get basic data
        funds_data = self._get_funds_basic_data(fund_isins)
        if not funds_data:
            return {"error": "No valid funds found for comparison"}
        
        comparison_result = {
            "comparison_date": datetime.utcnow().isoformat(),
            "comparison_type": comparison_type,
            "funds_count": len(fund_isins),
            "fund_summary": funds_data,
            "comparison_metrics": self._get_comparison_metrics(fund_isins, comparison_type),
            "performance_comparison": self._compare_performance(fund_isins),
            "risk_comparison": self._compare_risk_metrics(fund_isins),
            "cost_comparison": self._compare_costs(fund_isins),
            "portfolio_comparison": self._compare_portfolios(fund_isins),
            "ratings_comparison": self._compare_ratings(fund_isins),
            "ranking_analysis": self._rank_funds(fund_isins),
            "recommendation": self._generate_comparison_recommendation(fund_isins, funds_data)
        }
        
        return comparison_result
    
    def _get_funds_basic_data(self, fund_isins: List[str]) -> List[Dict]:
        """Get basic fund information for all funds"""
        funds_data = []
        
        for isin in fund_isins:
            fund = Fund.query.filter_by(isin=isin).first()
            if fund:
                factsheet = FundFactSheet.query.filter_by(isin=isin).first()
                funds_data.append({
                    "isin": isin,
                    "name": fund.scheme_name,
                    "amc": fund.amc_name,
                    "type": fund.fund_type,
                    "subtype": fund.fund_subtype,
                    "aum": factsheet.aum if factsheet else None,
                    "expense_ratio": factsheet.expense_ratio if factsheet else None,
                    "fund_manager": factsheet.fund_manager if factsheet else None,
                    "launch_date": factsheet.launch_date.isoformat() if factsheet and factsheet.launch_date else None
                })
            else:
                logger.warning(f"Fund with ISIN {isin} not found")
        
        return funds_data
    
    def _get_comparison_metrics(self, fund_isins: List[str], comparison_type: str) -> Dict:
        """Get specific metrics based on comparison type"""
        metrics = {
            "basic_info": self._get_basic_info_comparison(fund_isins),
            "key_metrics": self._get_key_metrics_comparison(fund_isins)
        }
        
        if comparison_type in ["comprehensive", "performance"]:
            metrics["performance_metrics"] = self._get_performance_metrics_comparison(fund_isins)
        
        if comparison_type in ["comprehensive", "risk"]:
            metrics["risk_metrics"] = self._get_risk_metrics_comparison(fund_isins)
        
        if comparison_type in ["comprehensive", "cost"]:
            metrics["cost_metrics"] = self._get_cost_metrics_comparison(fund_isins)
        
        return metrics
    
    def _get_basic_info_comparison(self, fund_isins: List[str]) -> Dict:
        """Compare basic fund information"""
        comparison = {}
        
        for isin in fund_isins:
            fund = Fund.query.filter_by(isin=isin).first()
            factsheet = FundFactSheet.query.filter_by(isin=isin).first()
            
            if fund:
                comparison[isin] = {
                    "fund_name": fund.scheme_name,
                    "amc_name": fund.amc_name,
                    "fund_type": fund.fund_type,
                    "fund_subtype": fund.fund_subtype,
                    "launch_date": factsheet.launch_date if factsheet else None,
                    "fund_manager": factsheet.fund_manager if factsheet else None,
                    "aum_crores": factsheet.aum if factsheet else None,
                    "exit_load": factsheet.exit_load if factsheet else None
                }
        
        return comparison
    
    def _get_key_metrics_comparison(self, fund_isins: List[str]) -> Dict:
        """Compare key fund metrics in tabular format"""
        metrics_table = []
        
        for isin in fund_isins:
            fund = Fund.query.filter_by(isin=isin).first()
            factsheet = FundFactSheet.query.filter_by(isin=isin).first()
            returns = FundReturns.query.filter_by(isin=isin).first()
            
            if fund:
                metrics_table.append({
                    "isin": isin,
                    "fund_name": fund.scheme_name,
                    "amc": fund.amc_name,
                    "aum_crores": factsheet.aum if factsheet else None,
                    "expense_ratio": factsheet.expense_ratio if factsheet else None,
                    "return_1y": returns.return_1y if returns else None,
                    "return_3y": returns.return_3y if returns else None,
                    "return_5y": returns.return_5y if returns else None
                })
        
        return {
            "metrics_table": metrics_table,
            "best_performers": self._identify_best_performers(metrics_table)
        }
    
    def _compare_performance(self, fund_isins: List[str]) -> Dict:
        """Compare performance metrics across funds"""
        performance_data = {}
        
        for isin in fund_isins:
            returns = FundReturns.query.filter_by(isin=isin).first()
            if returns:
                performance_data[isin] = {
                    "returns": {
                        "1_month": returns.return_1m,
                        "3_months": returns.return_3m,
                        "6_months": returns.return_6m,
                        "ytd": returns.return_ytd,
                        "1_year": returns.return_1y,
                        "3_years": returns.return_3y,
                        "5_years": returns.return_5y
                    },
                    "annualized_returns": {
                        "3_year_annualized": returns.return_3y / 3 if returns.return_3y else None,
                        "5_year_annualized": returns.return_5y / 5 if returns.return_5y else None
                    }
                }
        
        # Calculate performance rankings
        performance_rankings = self._calculate_performance_rankings(performance_data)
        
        # Calculate consistency metrics
        consistency_analysis = self._analyze_performance_consistency(performance_data)
        
        return {
            "performance_data": performance_data,
            "performance_rankings": performance_rankings,
            "consistency_analysis": consistency_analysis,
            "winner_analysis": self._identify_performance_winners(performance_data)
        }
    
    def _compare_risk_metrics(self, fund_isins: List[str]) -> Dict:
        """Compare risk metrics across funds"""
        risk_data = {}
        
        for isin in fund_isins:
            analytics = FundAnalytics.query.filter_by(isin=isin).order_by(FundAnalytics.calculation_date.desc()).first()
            
            if analytics:
                risk_data[isin] = {
                    "beta": analytics.beta,
                    "alpha": analytics.alpha,
                    "standard_deviation": analytics.standard_deviation,
                    "sharpe_ratio": analytics.sharpe_ratio,
                    "sortino_ratio": analytics.sortino_ratio,
                    "maximum_drawdown": analytics.maximum_drawdown,
                    "var_95": analytics.var_95,
                    "information_ratio": analytics.information_ratio,
                    "r_squared": analytics.r_squared
                }
            else:
                # Calculate basic risk from NAV if analytics not available
                risk_data[isin] = self._calculate_basic_risk_metrics(isin)
        
        risk_rankings = self._calculate_risk_rankings(risk_data)
        risk_grades = self._assign_risk_grades(risk_data)
        
        return {
            "risk_data": risk_data,
            "risk_rankings": risk_rankings,
            "risk_grades": risk_grades,
            "safest_fund": self._identify_safest_fund(risk_data),
            "highest_alpha": self._identify_highest_alpha(risk_data)
        }
    
    def _compare_costs(self, fund_isins: List[str]) -> Dict:
        """Compare cost-related metrics"""
        cost_data = {}
        
        for isin in fund_isins:
            factsheet = FundFactSheet.query.filter_by(isin=isin).first()
            if factsheet:
                cost_data[isin] = {
                    "expense_ratio": factsheet.expense_ratio,
                    "exit_load": factsheet.exit_load,
                    "aum_crores": factsheet.aum
                }
        
        cost_analysis = self._analyze_cost_efficiency(cost_data)
        
        return {
            "cost_data": cost_data,
            "cost_rankings": self._rank_by_cost(cost_data),
            "cost_efficiency": cost_analysis,
            "cheapest_fund": self._identify_cheapest_fund(cost_data)
        }
    
    def _compare_portfolios(self, fund_isins: List[str]) -> Dict:
        """Compare portfolio composition and holdings"""
        portfolio_data = {}
        
        for isin in fund_isins:
            statistics = FundStatistics.query.filter_by(isin=isin).order_by(FundStatistics.statistics_date.desc()).first()
            holdings = FundHolding.query.filter_by(isin=isin).all()
            
            portfolio_info = {
                "total_holdings": len(holdings),
                "top_10_holdings": self._get_top_holdings(holdings, 10),
                "sector_allocation": self._calculate_sector_allocation(holdings)
            }
            
            if statistics:
                portfolio_info.update({
                    "asset_allocation": {
                        "equity": statistics.equity_percentage,
                        "debt": statistics.debt_percentage,
                        "cash": statistics.cash_percentage
                    },
                    "market_cap_allocation": {
                        "large_cap": statistics.large_cap_percentage,
                        "mid_cap": statistics.mid_cap_percentage,
                        "small_cap": statistics.small_cap_percentage
                    },
                    "concentration_risk": statistics.top_10_holdings_percentage
                })
            
            portfolio_data[isin] = portfolio_info
        
        return {
            "portfolio_data": portfolio_data,
            "diversification_comparison": self._compare_diversification(portfolio_data),
            "concentration_analysis": self._analyze_concentration_risk(portfolio_data)
        }
    
    def _compare_ratings(self, fund_isins: List[str]) -> Dict:
        """Compare ratings across different agencies"""
        ratings_data = {}
        
        for isin in fund_isins:
            ratings = FundRating.query.filter_by(isin=isin, is_current=True).all()
            
            fund_ratings = {
                "devmani_recommended": False,
                "ratings_by_agency": {},
                "average_numeric_rating": None
            }
            
            numeric_ratings = []
            
            for rating in ratings:
                if rating.devmani_recommended:
                    fund_ratings["devmani_recommended"] = True
                
                if rating.rating_agency not in fund_ratings["ratings_by_agency"]:
                    fund_ratings["ratings_by_agency"][rating.rating_agency] = []
                
                rating_info = {
                    "category": rating.rating_category,
                    "value": rating.rating_value,
                    "numeric": rating.rating_numeric,
                    "date": rating.rating_date.isoformat() if rating.rating_date else None
                }
                
                fund_ratings["ratings_by_agency"][rating.rating_agency].append(rating_info)
                
                if rating.rating_numeric:
                    numeric_ratings.append(rating.rating_numeric)
            
            if numeric_ratings:
                fund_ratings["average_numeric_rating"] = np.mean(numeric_ratings)
            
            ratings_data[isin] = fund_ratings
        
        return {
            "ratings_data": ratings_data,
            "devmani_recommended_funds": [isin for isin, data in ratings_data.items() if data["devmani_recommended"]],
            "highest_rated_fund": self._identify_highest_rated_fund(ratings_data)
        }
    
    def _rank_funds(self, fund_isins: List[str]) -> Dict:
        """Create comprehensive fund rankings"""
        ranking_criteria = {
            "performance": self._rank_by_performance(fund_isins),
            "risk_adjusted_returns": self._rank_by_risk_adjusted_returns(fund_isins),
            "cost_efficiency": self._rank_by_cost_efficiency(fund_isins),
            "consistency": self._rank_by_consistency(fund_isins),
            "overall_score": self._calculate_overall_rankings(fund_isins)
        }
        
        return {
            "individual_rankings": ranking_criteria,
            "composite_ranking": self._create_composite_ranking(ranking_criteria),
            "ranking_explanation": self._explain_rankings(ranking_criteria)
        }
    
    def _generate_comparison_recommendation(self, fund_isins: List[str], funds_data: List[Dict]) -> Dict:
        """Generate investment recommendations based on comparison"""
        recommendations = []
        
        # Performance-based recommendation
        performance_winner = self._get_performance_winner(fund_isins)
        if performance_winner:
            recommendations.append({
                "type": "Performance Leader",
                "fund_isin": performance_winner,
                "reason": "Best overall performance across multiple time periods"
            })
        
        # Risk-adjusted recommendation
        risk_adjusted_winner = self._get_risk_adjusted_winner(fund_isins)
        if risk_adjusted_winner:
            recommendations.append({
                "type": "Risk-Adjusted Winner",
                "fund_isin": risk_adjusted_winner,
                "reason": "Best risk-adjusted returns (Sharpe ratio)"
            })
        
        # Cost-efficient recommendation
        cost_efficient = self._get_cost_efficient_fund(fund_isins)
        if cost_efficient:
            recommendations.append({
                "type": "Cost Efficient",
                "fund_isin": cost_efficient,
                "reason": "Lowest expense ratio with decent performance"
            })
        
        # Conservative recommendation
        conservative_choice = self._get_conservative_choice(fund_isins)
        if conservative_choice:
            recommendations.append({
                "type": "Conservative Choice",
                "fund_isin": conservative_choice,
                "reason": "Lowest volatility and drawdown risk"
            })
        
        # Overall recommendation
        overall_winner = self._get_overall_winner(fund_isins)
        
        return {
            "specific_recommendations": recommendations,
            "overall_winner": overall_winner,
            "investment_strategy_advice": self._generate_strategy_advice(fund_isins, funds_data),
            "portfolio_allocation_suggestion": self._suggest_allocation(fund_isins)
        }
    
    # Helper methods for calculations and analysis
    
    def _identify_best_performers(self, metrics_table: List[Dict]) -> Dict:
        """Identify best performers in key metrics"""
        best_performers = {}
        
        metrics_to_check = ["return_1y", "return_3y", "return_5y", "aum_crores"]
        
        for metric in metrics_to_check:
            valid_values = [(item["isin"], item[metric]) for item in metrics_table if item[metric] is not None]
            if valid_values:
                if metric == "aum_crores":
                    # Largest AUM
                    best = max(valid_values, key=lambda x: x[1])
                else:
                    # Highest return
                    best = max(valid_values, key=lambda x: x[1])
                
                best_performers[metric] = {
                    "isin": best[0],
                    "value": best[1]
                }
        
        return best_performers
    
    def _calculate_performance_rankings(self, performance_data: Dict) -> Dict:
        """Calculate performance rankings for different time periods"""
        rankings = {}
        
        time_periods = ["1_month", "3_months", "6_months", "1_year", "3_years", "5_years"]
        
        for period in time_periods:
            fund_returns = []
            for isin, data in performance_data.items():
                if data["returns"][period] is not None:
                    fund_returns.append((isin, data["returns"][period]))
            
            # Sort by returns (descending)
            fund_returns.sort(key=lambda x: x[1], reverse=True)
            
            rankings[period] = [{
                "rank": i + 1,
                "isin": isin,
                "return": return_value
            } for i, (isin, return_value) in enumerate(fund_returns)]
        
        return rankings
    
    def _analyze_performance_consistency(self, performance_data: Dict) -> Dict:
        """Analyze performance consistency across time periods"""
        consistency_scores = {}
        
        for isin, data in performance_data.items():
            returns = [data["returns"][period] for period in ["1_month", "3_months", "6_months", "1_year"] 
                      if data["returns"][period] is not None]
            
            if len(returns) >= 3:
                # Calculate coefficient of variation (lower = more consistent)
                mean_return = np.mean(returns)
                std_return = np.std(returns)
                cv = (std_return / abs(mean_return)) * 100 if mean_return != 0 else float('inf')
                
                consistency_scores[isin] = {
                    "coefficient_of_variation": cv,
                    "consistency_grade": "High" if cv < 50 else "Medium" if cv < 100 else "Low",
                    "return_count": len(returns),
                    "mean_return": mean_return,
                    "std_deviation": std_return
                }
        
        return consistency_scores
    
    def _identify_performance_winners(self, performance_data: Dict) -> Dict:
        """Identify winners in different categories"""
        winners = {}
        
        # Short-term winner (1 year)
        short_term_returns = [(isin, data["returns"]["1_year"]) for isin, data in performance_data.items() 
                             if data["returns"]["1_year"] is not None]
        if short_term_returns:
            winners["short_term"] = max(short_term_returns, key=lambda x: x[1])
        
        # Long-term winner (5 years)
        long_term_returns = [(isin, data["returns"]["5_years"]) for isin, data in performance_data.items() 
                            if data["returns"]["5_years"] is not None]
        if long_term_returns:
            winners["long_term"] = max(long_term_returns, key=lambda x: x[1])
        
        return winners
    
    def _calculate_basic_risk_metrics(self, isin: str) -> Dict:
        """Calculate basic risk metrics from NAV data"""
        nav_data = NavHistory.query.filter_by(isin=isin).order_by(NavHistory.date.desc()).limit(252).all()
        
        if len(nav_data) < 20:
            return {"error": "Insufficient data"}
        
        nav_values = [nav.nav for nav in reversed(nav_data)]
        returns = [(nav_values[i] - nav_values[i-1]) / nav_values[i-1] * 100 
                  for i in range(1, len(nav_values))]
        
        return {
            "standard_deviation": np.std(returns),
            "average_return": np.mean(returns),
            "maximum_drawdown": self._calculate_max_drawdown(nav_values),
            "calculated_from_nav": True
        }
    
    def _calculate_max_drawdown(self, nav_values: List[float]) -> float:
        """Calculate maximum drawdown"""
        if len(nav_values) < 2:
            return 0.0
        
        peak = nav_values[0]
        max_dd = 0.0
        
        for nav in nav_values[1:]:
            if nav > peak:
                peak = nav
            else:
                drawdown = (peak - nav) / peak * 100
                max_dd = max(max_dd, drawdown)
        
        return max_dd
    
    def _calculate_risk_rankings(self, risk_data: Dict) -> Dict:
        """Calculate risk-based rankings"""
        rankings = {}
        
        metrics = ["standard_deviation", "maximum_drawdown", "sharpe_ratio"]
        
        for metric in metrics:
            fund_values = []
            for isin, data in risk_data.items():
                if isinstance(data, dict) and metric in data and data[metric] is not None:
                    fund_values.append((isin, data[metric]))
            
            if fund_values:
                # For risk metrics, lower is better (except Sharpe ratio)
                reverse_sort = metric == "sharpe_ratio"
                fund_values.sort(key=lambda x: x[1], reverse=reverse_sort)
                
                rankings[metric] = [{
                    "rank": i + 1,
                    "isin": isin,
                    "value": value
                } for i, (isin, value) in enumerate(fund_values)]
        
        return rankings
    
    def _assign_risk_grades(self, risk_data: Dict) -> Dict:
        """Assign risk grades to funds"""
        risk_grades = {}
        
        for isin, data in risk_data.items():
            if isinstance(data, dict) and "standard_deviation" in data:
                std_dev = data["standard_deviation"]
                if std_dev is not None:
                    if std_dev < 10:
                        grade = "Low Risk"
                    elif std_dev < 20:
                        grade = "Medium Risk"
                    else:
                        grade = "High Risk"
                    
                    risk_grades[isin] = {
                        "grade": grade,
                        "standard_deviation": std_dev
                    }
        
        return risk_grades
    
    def _get_top_holdings(self, holdings: List, count: int = 10) -> List[Dict]:
        """Get top holdings by percentage"""
        if not holdings:
            return []
        
        sorted_holdings = sorted(holdings, key=lambda x: x.percentage_to_nav, reverse=True)
        
        return [{
            "name": holding.instrument_name,
            "percentage": holding.percentage_to_nav,
            "sector": holding.sector
        } for holding in sorted_holdings[:count]]
    
    def _calculate_sector_allocation(self, holdings: List) -> Dict:
        """Calculate sector-wise allocation"""
        sector_allocation = defaultdict(float)
        
        for holding in holdings:
            if holding.sector:
                sector_allocation[holding.sector] += holding.percentage_to_nav
        
        return dict(sector_allocation)
    
    def _rank_by_performance(self, fund_isins: List[str]) -> List[Dict]:
        """Rank funds by overall performance"""
        performance_scores = []
        
        for isin in fund_isins:
            returns = FundReturns.query.filter_by(isin=isin).first()
            if returns:
                # Weight different time periods
                score = 0
                weights = {"return_1y": 0.4, "return_3y": 0.4, "return_5y": 0.2}
                
                for period, weight in weights.items():
                    return_value = getattr(returns, period, None)
                    if return_value is not None:
                        score += return_value * weight
                
                performance_scores.append({"isin": isin, "score": score})
        
        performance_scores.sort(key=lambda x: x["score"], reverse=True)
        return performance_scores
    
    def _get_performance_winner(self, fund_isins: List[str]) -> Optional[str]:
        """Get the overall performance winner"""
        performance_ranking = self._rank_by_performance(fund_isins)
        return performance_ranking[0]["isin"] if performance_ranking else None
    
    def _get_risk_adjusted_winner(self, fund_isins: List[str]) -> Optional[str]:
        """Get the best risk-adjusted returns winner"""
        best_sharpe = None
        winner = None
        
        for isin in fund_isins:
            analytics = FundAnalytics.query.filter_by(isin=isin).order_by(FundAnalytics.calculation_date.desc()).first()
            if analytics and analytics.sharpe_ratio:
                if best_sharpe is None or analytics.sharpe_ratio > best_sharpe:
                    best_sharpe = analytics.sharpe_ratio
                    winner = isin
        
        return winner
    
    def _get_cost_efficient_fund(self, fund_isins: List[str]) -> Optional[str]:
        """Get the most cost-efficient fund"""
        lowest_expense = None
        winner = None
        
        for isin in fund_isins:
            factsheet = FundFactSheet.query.filter_by(isin=isin).first()
            if factsheet and factsheet.expense_ratio:
                if lowest_expense is None or factsheet.expense_ratio < lowest_expense:
                    lowest_expense = factsheet.expense_ratio
                    winner = isin
        
        return winner
    
    def _get_conservative_choice(self, fund_isins: List[str]) -> Optional[str]:
        """Get the most conservative fund choice"""
        lowest_risk = None
        winner = None
        
        for isin in fund_isins:
            analytics = FundAnalytics.query.filter_by(isin=isin).order_by(FundAnalytics.calculation_date.desc()).first()
            if analytics and analytics.standard_deviation:
                if lowest_risk is None or analytics.standard_deviation < lowest_risk:
                    lowest_risk = analytics.standard_deviation
                    winner = isin
        
        return winner
    
    def _get_overall_winner(self, fund_isins: List[str]) -> Optional[Dict]:
        """Get the overall winner based on composite scoring"""
        composite_ranking = self._calculate_overall_rankings(fund_isins)
        if composite_ranking:
            return {
                "isin": composite_ranking[0]["isin"],
                "score": composite_ranking[0]["score"],
                "reason": "Best overall score considering performance, risk, and cost factors"
            }
        return None
    
    def _calculate_overall_rankings(self, fund_isins: List[str]) -> List[Dict]:
        """Calculate overall composite rankings"""
        composite_scores = []
        
        for isin in fund_isins:
            score = 0
            components = {}
            
            # Performance component (40%)
            returns = FundReturns.query.filter_by(isin=isin).first()
            if returns and returns.return_1y:
                performance_score = min(100, max(0, returns.return_1y * 2))  # Scale to 0-100
                score += performance_score * 0.4
                components["performance"] = performance_score
            
            # Risk component (30%)
            analytics = FundAnalytics.query.filter_by(isin=isin).order_by(FundAnalytics.calculation_date.desc()).first()
            if analytics and analytics.sharpe_ratio:
                risk_score = min(100, max(0, analytics.sharpe_ratio * 20))  # Scale to 0-100
                score += risk_score * 0.3
                components["risk_adjusted"] = risk_score
            
            # Cost component (20%)
            factsheet = FundFactSheet.query.filter_by(isin=isin).first()
            if factsheet and factsheet.expense_ratio:
                cost_score = max(0, 100 - factsheet.expense_ratio * 50)  # Lower expense = higher score
                score += cost_score * 0.2
                components["cost"] = cost_score
            
            # Rating component (10%)
            ratings = FundRating.query.filter_by(isin=isin, is_current=True).all()
            if ratings:
                avg_rating = np.mean([r.rating_numeric for r in ratings if r.rating_numeric])
                rating_score = (avg_rating / 5) * 100 if avg_rating else 50
                score += rating_score * 0.1
                components["rating"] = rating_score
            
            if score > 0:
                composite_scores.append({
                    "isin": isin,
                    "score": score,
                    "components": components
                })
        
        composite_scores.sort(key=lambda x: x["score"], reverse=True)
        return composite_scores
    
    def _generate_strategy_advice(self, fund_isins: List[str], funds_data: List[Dict]) -> List[str]:
        """Generate investment strategy advice"""
        advice = []
        
        # Check fund type diversity
        fund_types = set(fund["type"] for fund in funds_data)
        if len(fund_types) == 1:
            advice.append("Consider diversifying across different fund types (equity, debt, hybrid) for balanced risk.")
        
        # Check AMC diversity
        amcs = set(fund["amc"] for fund in funds_data)
        if len(amcs) < len(funds_data) / 2:
            advice.append("Consider spreading investments across different AMCs to reduce concentration risk.")
        
        # Performance-based advice
        performance_data = self._compare_performance(fund_isins)
        if performance_data.get("winner_analysis"):
            advice.append("Focus on long-term performers for better wealth creation over time.")
        
        return advice
    
    def _suggest_allocation(self, fund_isins: List[str]) -> Dict:
        """Suggest portfolio allocation based on fund analysis"""
        allocation_suggestion = {}
        total_funds = len(fund_isins)
        
        if total_funds <= 2:
            # Equal allocation for 2 funds
            for isin in fund_isins:
                allocation_suggestion[isin] = 50.0
        elif total_funds <= 4:
            # Performance-weighted allocation
            performance_ranking = self._rank_by_performance(fund_isins)
            weights = [40, 30, 20, 10]
            
            for i, fund_data in enumerate(performance_ranking):
                if i < len(weights):
                    allocation_suggestion[fund_data["isin"]] = weights[i]
        else:
            # Equal allocation for more than 4 funds
            equal_weight = 100.0 / total_funds
            for isin in fund_isins:
                allocation_suggestion[isin] = equal_weight
        
        return {
            "suggested_allocation": allocation_suggestion,
            "allocation_strategy": "Performance-weighted" if total_funds <= 4 else "Equal-weighted",
            "rebalancing_frequency": "Quarterly"
        }
    
    # Additional helper methods for completeness
    def _analyze_cost_efficiency(self, cost_data: Dict) -> Dict:
        """Analyze cost efficiency across funds"""
        if not cost_data:
            return {}
        
        expense_ratios = [data["expense_ratio"] for data in cost_data.values() if data.get("expense_ratio")]
        
        if not expense_ratios:
            return {"error": "No expense ratio data available"}
        
        return {
            "average_expense_ratio": np.mean(expense_ratios),
            "lowest_expense_ratio": min(expense_ratios),
            "highest_expense_ratio": max(expense_ratios),
            "expense_ratio_range": max(expense_ratios) - min(expense_ratios)
        }
    
    def _rank_by_cost(self, cost_data: Dict) -> List[Dict]:
        """Rank funds by cost efficiency"""
        cost_ranking = []
        
        for isin, data in cost_data.items():
            if data.get("expense_ratio"):
                cost_ranking.append({
                    "isin": isin,
                    "expense_ratio": data["expense_ratio"]
                })
        
        cost_ranking.sort(key=lambda x: x["expense_ratio"])
        return cost_ranking
    
    def _identify_cheapest_fund(self, cost_data: Dict) -> Optional[str]:
        """Identify the cheapest fund by expense ratio"""
        cheapest = None
        lowest_expense = None
        
        for isin, data in cost_data.items():
            if data.get("expense_ratio"):
                if lowest_expense is None or data["expense_ratio"] < lowest_expense:
                    lowest_expense = data["expense_ratio"]
                    cheapest = isin
        
        return cheapest
    
    def _compare_diversification(self, portfolio_data: Dict) -> Dict:
        """Compare diversification across funds"""
        diversification_scores = {}
        
        for isin, data in portfolio_data.items():
            score = 0
            
            # Holdings count component
            holdings_count = data.get("total_holdings", 0)
            if holdings_count > 50:
                score += 30
            elif holdings_count > 30:
                score += 20
            elif holdings_count > 15:
                score += 10
            
            # Sector diversification component
            sectors = len(data.get("sector_allocation", {}))
            if sectors > 10:
                score += 30
            elif sectors > 7:
                score += 20
            elif sectors > 5:
                score += 10
            
            # Concentration risk component
            concentration = data.get("concentration_risk", 100)
            if concentration < 30:
                score += 40
            elif concentration < 50:
                score += 25
            elif concentration < 70:
                score += 15
            
            diversification_scores[isin] = {
                "score": score,
                "grade": "Excellent" if score >= 80 else "Good" if score >= 60 else "Average" if score >= 40 else "Poor"
            }
        
        return diversification_scores
    
    def _analyze_concentration_risk(self, portfolio_data: Dict) -> Dict:
        """Analyze concentration risk across funds"""
        concentration_analysis = {}
        
        for isin, data in portfolio_data.items():
            concentration_risk = data.get("concentration_risk", 0)
            
            if concentration_risk > 70:
                risk_level = "High"
                advice = "High concentration in top holdings may increase portfolio risk"
            elif concentration_risk > 50:
                risk_level = "Medium"
                advice = "Moderate concentration - monitor for changes"
            else:
                risk_level = "Low"
                advice = "Well-diversified holdings reduce concentration risk"
            
            concentration_analysis[isin] = {
                "concentration_percentage": concentration_risk,
                "risk_level": risk_level,
                "advice": advice
            }
        
        return concentration_analysis
    
    def _identify_safest_fund(self, risk_data: Dict) -> Optional[str]:
        """Identify the safest fund based on risk metrics"""
        safest = None
        lowest_risk_score = None
        
        for isin, data in risk_data.items():
            if isinstance(data, dict):
                risk_score = 0
                
                # Standard deviation component
                if data.get("standard_deviation"):
                    risk_score += data["standard_deviation"]
                
                # Maximum drawdown component
                if data.get("maximum_drawdown"):
                    risk_score += data["maximum_drawdown"]
                
                if risk_score > 0:
                    if lowest_risk_score is None or risk_score < lowest_risk_score:
                        lowest_risk_score = risk_score
                        safest = isin
        
        return safest
    
    def _identify_highest_alpha(self, risk_data: Dict) -> Optional[str]:
        """Identify fund with highest alpha"""
        highest_alpha = None
        alpha_winner = None
        
        for isin, data in risk_data.items():
            if isinstance(data, dict) and data.get("alpha"):
                if highest_alpha is None or data["alpha"] > highest_alpha:
                    highest_alpha = data["alpha"]
                    alpha_winner = isin
        
        return alpha_winner
    
    def _identify_highest_rated_fund(self, ratings_data: Dict) -> Optional[str]:
        """Identify the highest rated fund"""
        highest_rating = None
        winner = None
        
        for isin, data in ratings_data.items():
            avg_rating = data.get("average_numeric_rating")
            if avg_rating and (highest_rating is None or avg_rating > highest_rating):
                highest_rating = avg_rating
                winner = isin
        
        return winner
    
    def _rank_by_risk_adjusted_returns(self, fund_isins: List[str]) -> List[Dict]:
        """Rank funds by risk-adjusted returns"""
        sharpe_rankings = []
        
        for isin in fund_isins:
            analytics = FundAnalytics.query.filter_by(isin=isin).order_by(FundAnalytics.calculation_date.desc()).first()
            if analytics and analytics.sharpe_ratio:
                sharpe_rankings.append({
                    "isin": isin,
                    "sharpe_ratio": analytics.sharpe_ratio
                })
        
        sharpe_rankings.sort(key=lambda x: x["sharpe_ratio"], reverse=True)
        return sharpe_rankings
    
    def _rank_by_cost_efficiency(self, fund_isins: List[str]) -> List[Dict]:
        """Rank funds by cost efficiency"""
        cost_rankings = []
        
        for isin in fund_isins:
            factsheet = FundFactSheet.query.filter_by(isin=isin).first()
            if factsheet and factsheet.expense_ratio:
                cost_rankings.append({
                    "isin": isin,
                    "expense_ratio": factsheet.expense_ratio
                })
        
        cost_rankings.sort(key=lambda x: x["expense_ratio"])
        return cost_rankings
    
    def _rank_by_consistency(self, fund_isins: List[str]) -> List[Dict]:
        """Rank funds by performance consistency"""
        consistency_rankings = []
        
        for isin in fund_isins:
            returns = FundReturns.query.filter_by(isin=isin).first()
            if returns:
                return_values = [returns.return_1m, returns.return_3m, returns.return_6m, returns.return_1y]
                return_values = [r for r in return_values if r is not None]
                
                if len(return_values) >= 3:
                    cv = (np.std(return_values) / abs(np.mean(return_values))) * 100 if np.mean(return_values) != 0 else float('inf')
                    consistency_rankings.append({
                        "isin": isin,
                        "consistency_score": 100 - min(100, cv)  # Higher score = more consistent
                    })
        
        consistency_rankings.sort(key=lambda x: x["consistency_score"], reverse=True)
        return consistency_rankings
    
    def _create_composite_ranking(self, ranking_criteria: Dict) -> List[Dict]:
        """Create composite ranking from individual criteria"""
        fund_scores = defaultdict(float)
        weights = {
            "performance": 0.35,
            "risk_adjusted_returns": 0.25,
            "cost_efficiency": 0.20,
            "consistency": 0.20
        }
        
        for criterion, weight in weights.items():
            if criterion in ranking_criteria:
                rankings = ranking_criteria[criterion]
                for i, fund_data in enumerate(rankings):
                    isin = fund_data["isin"]
                    # Higher rank = lower score (1st place gets highest points)
                    points = max(0, len(rankings) - i)
                    fund_scores[isin] += points * weight
        
        composite_ranking = [{"isin": isin, "composite_score": score} for isin, score in fund_scores.items()]
        composite_ranking.sort(key=lambda x: x["composite_score"], reverse=True)
        
        return composite_ranking
    
    def _explain_rankings(self, ranking_criteria: Dict) -> Dict:
        """Explain the ranking methodology"""
        return {
            "methodology": "Composite ranking based on weighted criteria",
            "weights": {
                "Performance": "35% - Based on 1Y, 3Y, 5Y returns",
                "Risk-Adjusted Returns": "25% - Based on Sharpe ratio",
                "Cost Efficiency": "20% - Based on expense ratio",
                "Consistency": "20% - Based on return consistency"
            },
            "scoring": "Higher scores indicate better performance in each category"
        }

# Main usage functions
def compare_funds_comprehensive(fund_isins: List[str]) -> Dict:
    """
    Comprehensive fund comparison
    
    Args:
        fund_isins: List of fund ISINs to compare
        
    Returns:
        Dict: Complete comparison analysis
        
    Usage:
        comparison = compare_funds_comprehensive([
            "INF090I01239", 
            "INF090I01247", 
            "INF090I01255"
        ])
    """
    tool = FundComparisonTool()
    return tool.compare_funds(fund_isins, "comprehensive")

def compare_funds_performance(fund_isins: List[str]) -> Dict:
    """Performance-focused comparison"""
    tool = FundComparisonTool()
    return tool.compare_funds(fund_isins, "performance")

def compare_funds_risk(fund_isins: List[str]) -> Dict:
    """Risk-focused comparison"""
    tool = FundComparisonTool()
    return tool.compare_funds(fund_isins, "risk")

def compare_funds_cost(fund_isins: List[str]) -> Dict:
    """Cost-focused comparison"""
    tool = FundComparisonTool()
    return tool.compare_funds(fund_isins, "cost")

if __name__ == "__main__":
    # Example usage
    sample_funds = ["INF090I01239", "INF090I01247", "INF090I01255"]  # Replace with actual ISINs
    
    print("=== Fund Comparison Analysis ===")
    comparison_result = compare_funds_comprehensive(sample_funds)
    
    if "error" not in comparison_result:
        print(f"Funds compared: {comparison_result['funds_count']}")
        print(f"Overall winner: {comparison_result['recommendation']['overall_winner']['isin'] if comparison_result['recommendation']['overall_winner'] else 'None'}")
        print("\nTop recommendations:")
        for rec in comparison_result['recommendation']['specific_recommendations']:
            print(f"- {rec['type']}: {rec['fund_isin']} - {rec['reason']}")
    else:
        print(f"Error: {comparison_result['error']}")