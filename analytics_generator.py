"""
Analytics Generator for Mutual Funds
Generates comprehensive analytics for a given ISIN using existing fund data
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from sqlalchemy import and_, desc

from setup_db import db
from models import (
    Fund, FundFactSheet, FundReturns, FundHolding, NavHistory,
    FundRating, FundAnalytics, FundStatistics, FundCodeLookup
)

logger = logging.getLogger(__name__)

class FundAnalyticsGenerator:
    """
    Generate comprehensive analytics for mutual funds based on ISIN
    """
    
    def __init__(self):
        """Initialize the analytics generator"""
        pass
    
    def generate_comprehensive_analytics(self, isin: str) -> Dict:
        """
        Generate complete analytics report for a given ISIN
        
        Args:
            isin (str): Fund ISIN code
            
        Returns:
            Dict: Comprehensive analytics data
        """
        logger.info(f"Generating analytics for ISIN: {isin}")
        
        # Verify fund exists
        fund = Fund.query.filter_by(isin=isin).first()
        if not fund:
            return {"error": f"Fund with ISIN {isin} not found"}
        
        analytics_data = {
            "isin": isin,
            "fund_name": fund.scheme_name,
            "amc_name": fund.amc_name,
            "fund_type": fund.fund_type,
            "generated_at": datetime.utcnow().isoformat(),
            "basic_info": self._get_basic_fund_info(fund),
            "performance_metrics": self._calculate_performance_metrics(isin),
            "risk_analytics": self._calculate_risk_analytics(isin),
            "portfolio_analysis": self._analyze_portfolio_composition(isin),
            "comparative_analysis": self._get_comparative_analysis(isin),
            "flow_analysis": self._analyze_fund_flows(isin),
            "rating_summary": self._get_rating_summary(isin),
            "nav_trends": self._analyze_nav_trends(isin),
            "sector_allocation": self._analyze_sector_allocation(isin),
            "recommendation_status": self._get_recommendation_status(isin)
        }
        
        return analytics_data
    
    def _get_basic_fund_info(self, fund: Fund) -> Dict:
        """Get basic fund information"""
        factsheet = fund.factsheet
        if not factsheet:
            return {"error": "Factsheet data not available"}
        
        return {
            "fund_manager": factsheet.fund_manager,
            "aum_crores": factsheet.aum,
            "expense_ratio": factsheet.expense_ratio,
            "launch_date": factsheet.launch_date.isoformat() if factsheet.launch_date else None,
            "exit_load": factsheet.exit_load,
            "last_updated": factsheet.last_updated.isoformat()
        }
    
    def _calculate_performance_metrics(self, isin: str) -> Dict:
        """Calculate performance metrics"""
        returns_data = FundReturns.query.filter_by(isin=isin).first()
        if not returns_data:
            return {"error": "Returns data not available"}
        
        return {
            "returns": {
                "1_month": returns_data.return_1m,
                "3_months": returns_data.return_3m,
                "6_months": returns_data.return_6m,
                "ytd": returns_data.return_ytd,
                "1_year": returns_data.return_1y,
                "3_years": returns_data.return_3y,
                "5_years": returns_data.return_5y
            },
            "annualized_returns": {
                "3_years_annualized": returns_data.return_3y / 3 if returns_data.return_3y else None,
                "5_years_annualized": returns_data.return_5y / 5 if returns_data.return_5y else None
            },
            "volatility_assessment": self._assess_volatility(returns_data),
            "consistency_score": self._calculate_consistency_score(returns_data)
        }
    
    def _calculate_risk_analytics(self, isin: str) -> Dict:
        """Calculate risk-based analytics"""
        analytics = FundAnalytics.query.filter_by(isin=isin).order_by(desc(FundAnalytics.calculation_date)).first()
        
        if not analytics:
            # Calculate basic risk metrics from NAV data if analytics not available
            return self._calculate_basic_risk_from_nav(isin)
        
        return {
            "risk_metrics": {
                "beta": analytics.beta,
                "alpha": analytics.alpha,
                "sharpe_ratio": analytics.sharpe_ratio,
                "sortino_ratio": analytics.sortino_ratio,
                "standard_deviation": analytics.standard_deviation,
                "maximum_drawdown": analytics.maximum_drawdown
            },
            "market_timing": {
                "up_capture": analytics.up_capture_ratio,
                "down_capture": analytics.down_capture_ratio,
                "information_ratio": analytics.information_ratio
            },
            "value_at_risk": {
                "var_95": analytics.var_95,
                "var_99": analytics.var_99
            },
            "risk_grade": self._assign_risk_grade(analytics),
            "benchmark": analytics.benchmark_index,
            "calculation_date": analytics.calculation_date.isoformat()
        }
    
    def _analyze_portfolio_composition(self, isin: str) -> Dict:
        """Analyze portfolio composition"""
        statistics = FundStatistics.query.filter_by(isin=isin).order_by(desc(FundStatistics.statistics_date)).first()
        holdings = FundHolding.query.filter_by(isin=isin).all()
        
        composition_data = {
            "total_holdings": len(holdings) if holdings else 0,
            "top_holdings": self._get_top_holdings(holdings),
            "diversification_score": self._calculate_diversification_score(holdings)
        }
        
        if statistics:
            composition_data.update({
                "asset_allocation": {
                    "equity": statistics.equity_percentage,
                    "debt": statistics.debt_percentage,
                    "cash": statistics.cash_percentage,
                    "others": statistics.other_percentage
                },
                "market_cap_distribution": {
                    "large_cap": statistics.large_cap_percentage,
                    "mid_cap": statistics.mid_cap_percentage,
                    "small_cap": statistics.small_cap_percentage
                },
                "concentration_risk": {
                    "top_10_holdings": statistics.top_10_holdings_percentage,
                    "top_sector": statistics.top_sector_name,
                    "top_sector_weight": statistics.top_sector_percentage
                }
            })
        
        return composition_data
    
    def _get_comparative_analysis(self, isin: str) -> Dict:
        """Compare fund with category peers"""
        fund = Fund.query.filter_by(isin=isin).first()
        
        # Get category peers
        category_peers = Fund.query.filter(
            and_(
                Fund.fund_type == fund.fund_type,
                Fund.isin != isin
            )
        ).limit(10).all()
        
        peer_returns = []
        for peer in category_peers:
            peer_return = FundReturns.query.filter_by(isin=peer.isin).first()
            if peer_return:
                peer_returns.append({
                    "isin": peer.isin,
                    "name": peer.scheme_name,
                    "return_1y": peer_return.return_1y,
                    "return_3y": peer_return.return_3y
                })
        
        current_returns = FundReturns.query.filter_by(isin=isin).first()
        
        return {
            "category": fund.fund_type,
            "peer_comparison": {
                "total_peers_analyzed": len(peer_returns),
                "fund_rank_1y": self._calculate_rank(current_returns.return_1y if current_returns else None, 
                                                   [p["return_1y"] for p in peer_returns]),
                "fund_rank_3y": self._calculate_rank(current_returns.return_3y if current_returns else None,
                                                   [p["return_3y"] for p in peer_returns]),
                "category_average_1y": np.mean([p["return_1y"] for p in peer_returns if p["return_1y"]]) if peer_returns else None,
                "category_average_3y": np.mean([p["return_3y"] for p in peer_returns if p["return_3y"]]) if peer_returns else None
            },
            "top_performers": peer_returns[:5]
        }
    
    def _analyze_fund_flows(self, isin: str) -> Dict:
        """Analyze fund flow patterns"""
        statistics = FundStatistics.query.filter_by(isin=isin).order_by(desc(FundStatistics.statistics_date)).first()
        
        if not statistics:
            return {"error": "Flow data not available"}
        
        return {
            "recent_flows": {
                "monthly_net_flow": statistics.net_flow,
                "quarterly_flow": statistics.quarterly_flow,
                "yearly_flow": statistics.yearly_flow
            },
            "flow_trend": self._assess_flow_trend(statistics),
            "investor_sentiment": self._assess_investor_sentiment(statistics)
        }
    
    def _get_rating_summary(self, isin: str) -> Dict:
        """Get comprehensive rating summary"""
        ratings = FundRating.query.filter_by(isin=isin, is_current=True).all()
        
        rating_summary = {
            "total_ratings": len(ratings),
            "devmani_recommended": False,
            "ratings_by_agency": {}
        }
        
        for rating in ratings:
            if rating.devmani_recommended:
                rating_summary["devmani_recommended"] = True
            
            if rating.rating_agency not in rating_summary["ratings_by_agency"]:
                rating_summary["ratings_by_agency"][rating.rating_agency] = []
            
            rating_summary["ratings_by_agency"][rating.rating_agency].append({
                "category": rating.rating_category,
                "value": rating.rating_value,
                "numeric": rating.rating_numeric,
                "outlook": rating.rating_outlook,
                "date": rating.rating_date.isoformat()
            })
        
        return rating_summary
    
    def _analyze_nav_trends(self, isin: str) -> Dict:
        """Analyze NAV trends and patterns"""
        # Get last 1 year of NAV data
        one_year_ago = datetime.utcnow().date() - timedelta(days=365)
        nav_data = NavHistory.query.filter(
            and_(
                NavHistory.isin == isin,
                NavHistory.date >= one_year_ago
            )
        ).order_by(NavHistory.date.desc()).all()
        
        if not nav_data:
            return {"error": "NAV history not available"}
        
        nav_values = [nav.nav for nav in nav_data]
        nav_dates = [nav.date for nav in nav_data]
        
        return {
            "current_nav": nav_values[0] if nav_values else None,
            "52_week_high": max(nav_values) if nav_values else None,
            "52_week_low": min(nav_values) if nav_values else None,
            "nav_volatility": np.std(nav_values) if len(nav_values) > 1 else None,
            "trend_analysis": self._analyze_trend(nav_values),
            "data_points": len(nav_data)
        }
    
    def _analyze_sector_allocation(self, isin: str) -> Dict:
        """Analyze sector-wise allocation"""
        holdings = FundHolding.query.filter_by(isin=isin).all()
        
        sector_allocation = {}
        for holding in holdings:
            if holding.sector:
                if holding.sector not in sector_allocation:
                    sector_allocation[holding.sector] = 0
                sector_allocation[holding.sector] += holding.percentage_to_nav
        
        # Sort by allocation
        sorted_sectors = sorted(sector_allocation.items(), key=lambda x: x[1], reverse=True)
        
        return {
            "sector_count": len(sector_allocation),
            "top_sectors": sorted_sectors[:10],
            "sector_diversification": self._calculate_sector_diversification(sector_allocation)
        }
    
    def _get_recommendation_status(self, isin: str) -> Dict:
        """Get recommendation status across different criteria"""
        # Check if fund has Devmani recommendation
        devmani_rating = FundRating.query.filter_by(
            isin=isin, 
            devmani_recommended=True, 
            is_current=True
        ).first()
        
        # Get overall scores
        analytics = FundAnalytics.query.filter_by(isin=isin).order_by(desc(FundAnalytics.calculation_date)).first()
        returns = FundReturns.query.filter_by(isin=isin).first()
        
        recommendation_score = self._calculate_recommendation_score(analytics, returns)
        
        return {
            "devmani_recommended": devmani_rating is not None,
            "overall_score": recommendation_score,
            "recommendation_grade": self._assign_recommendation_grade(recommendation_score),
            "key_strengths": self._identify_key_strengths(analytics, returns),
            "areas_of_concern": self._identify_concerns(analytics, returns)
        }
    
    # Helper methods for calculations
    def _assess_volatility(self, returns_data) -> str:
        """Assess volatility based on returns variation"""
        returns = [returns_data.return_1m, returns_data.return_3m, returns_data.return_6m, returns_data.return_1y]
        returns = [r for r in returns if r is not None]
        
        if len(returns) < 2:
            return "Insufficient data"
        
        volatility = np.std(returns)
        if volatility < 5:
            return "Low"
        elif volatility < 15:
            return "Moderate"
        else:
            return "High"
    
    def _calculate_consistency_score(self, returns_data) -> float:
        """Calculate consistency score (0-100)"""
        returns = [returns_data.return_1m, returns_data.return_3m, returns_data.return_6m, returns_data.return_1y]
        returns = [r for r in returns if r is not None and r > 0]
        
        if len(returns) < 2:
            return 0.0
        
        # Higher consistency = lower variation in positive returns
        positive_returns = [r for r in returns if r > 0]
        if len(positive_returns) < 2:
            return 0.0
        
        consistency = 100 - min(100, np.std(positive_returns) * 10)
        return max(0, consistency)
    
    def _calculate_basic_risk_from_nav(self, isin: str) -> Dict:
        """Calculate basic risk metrics from NAV data when analytics not available"""
        nav_data = NavHistory.query.filter_by(isin=isin).order_by(NavHistory.date.desc()).limit(252).all()  # ~1 year
        
        if len(nav_data) < 20:
            return {"error": "Insufficient NAV data for risk calculation"}
        
        nav_values = [nav.nav for nav in reversed(nav_data)]
        returns = [(nav_values[i] - nav_values[i-1]) / nav_values[i-1] * 100 
                  for i in range(1, len(nav_values))]
        
        return {
            "calculated_from_nav": True,
            "standard_deviation": np.std(returns),
            "maximum_drawdown": self._calculate_max_drawdown(nav_values),
            "average_return": np.mean(returns),
            "data_period_days": len(nav_data)
        }
    
    def _calculate_max_drawdown(self, nav_values: List[float]) -> float:
        """Calculate maximum drawdown from NAV values"""
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
    
    def _assign_risk_grade(self, analytics) -> str:
        """Assign risk grade based on analytics"""
        if not analytics.standard_deviation:
            return "Not Available"
        
        if analytics.standard_deviation < 10:
            return "Low Risk"
        elif analytics.standard_deviation < 20:
            return "Moderate Risk"
        else:
            return "High Risk"
    
    def _get_top_holdings(self, holdings: List[FundHolding]) -> List[Dict]:
        """Get top 10 holdings"""
        if not holdings:
            return []
        
        sorted_holdings = sorted(holdings, key=lambda x: x.percentage_to_nav, reverse=True)
        
        return [{
            "name": holding.instrument_name,
            "percentage": holding.percentage_to_nav,
            "sector": holding.sector,
            "type": holding.instrument_type
        } for holding in sorted_holdings[:10]]
    
    def _calculate_diversification_score(self, holdings: List[FundHolding]) -> float:
        """Calculate diversification score (0-100)"""
        if not holdings:
            return 0.0
        
        # Simple diversification score based on holding concentration
        total_holdings = len(holdings)
        if total_holdings < 10:
            return 20.0
        elif total_holdings < 30:
            return 60.0
        else:
            return 90.0
    
    def _calculate_rank(self, fund_value: float, peer_values: List[float]) -> Optional[int]:
        """Calculate fund rank among peers"""
        if fund_value is None or not peer_values:
            return None
        
        valid_peers = [v for v in peer_values if v is not None]
        if not valid_peers:
            return None
        
        better_performers = len([v for v in valid_peers if v > fund_value])
        return better_performers + 1
    
    def _assess_flow_trend(self, statistics) -> str:
        """Assess fund flow trend"""
        if statistics.yearly_flow is None:
            return "Data not available"
        
        if statistics.yearly_flow > 1000:  # 1000 crores
            return "Strong Inflows"
        elif statistics.yearly_flow > 0:
            return "Positive Inflows"
        elif statistics.yearly_flow > -500:
            return "Moderate Outflows"
        else:
            return "Heavy Outflows"
    
    def _assess_investor_sentiment(self, statistics) -> str:
        """Assess investor sentiment based on flows"""
        recent_flow = statistics.net_flow or 0
        
        if recent_flow > 100:
            return "Very Positive"
        elif recent_flow > 0:
            return "Positive"
        elif recent_flow > -50:
            return "Neutral"
        else:
            return "Negative"
    
    def _analyze_trend(self, nav_values: List[float]) -> str:
        """Analyze NAV trend"""
        if len(nav_values) < 10:
            return "Insufficient data"
        
        # Simple trend analysis - compare first and last 10% of data
        start_avg = np.mean(nav_values[-10:])  # Latest values
        end_avg = np.mean(nav_values[:10])     # Oldest values
        
        change_pct = (start_avg - end_avg) / end_avg * 100
        
        if change_pct > 5:
            return "Strong Uptrend"
        elif change_pct > 0:
            return "Uptrend"
        elif change_pct > -5:
            return "Sideways"
        else:
            return "Downtrend"
    
    def _calculate_sector_diversification(self, sector_allocation: Dict) -> float:
        """Calculate sector diversification score"""
        if not sector_allocation:
            return 0.0
        
        # Higher number of sectors with balanced allocation = better diversification
        sector_count = len(sector_allocation)
        max_allocation = max(sector_allocation.values()) if sector_allocation else 100
        
        if sector_count < 3:
            return 20.0
        elif max_allocation > 50:
            return 40.0
        elif max_allocation > 30:
            return 70.0
        else:
            return 90.0
    
    def _calculate_recommendation_score(self, analytics, returns) -> float:
        """Calculate overall recommendation score (0-100)"""
        score = 50.0  # Base score
        
        if returns:
            # Performance component (30%)
            if returns.return_1y and returns.return_1y > 15:
                score += 15
            elif returns.return_1y and returns.return_1y > 10:
                score += 10
            elif returns.return_1y and returns.return_1y > 5:
                score += 5
            
            # Consistency component (20%)
            consistency = self._calculate_consistency_score(returns)
            score += (consistency / 100) * 20
        
        if analytics:
            # Risk-adjusted returns (30%)
            if analytics.sharpe_ratio and analytics.sharpe_ratio > 1:
                score += 15
            elif analytics.sharpe_ratio and analytics.sharpe_ratio > 0.5:
                score += 10
            
            # Risk management (20%)
            if analytics.maximum_drawdown and analytics.maximum_drawdown < 10:
                score += 10
            elif analytics.maximum_drawdown and analytics.maximum_drawdown < 20:
                score += 5
        
        return min(100, max(0, score))
    
    def _assign_recommendation_grade(self, score: float) -> str:
        """Assign recommendation grade based on score"""
        if score >= 80:
            return "Strong Buy"
        elif score >= 65:
            return "Buy"
        elif score >= 50:
            return "Hold"
        elif score >= 35:
            return "Weak Hold"
        else:
            return "Avoid"
    
    def _identify_key_strengths(self, analytics, returns) -> List[str]:
        """Identify key strengths of the fund"""
        strengths = []
        
        if returns:
            if returns.return_1y and returns.return_1y > 15:
                strengths.append("Strong 1-year performance")
            if returns.return_3y and returns.return_3y > 12:
                strengths.append("Consistent long-term returns")
        
        if analytics:
            if analytics.sharpe_ratio and analytics.sharpe_ratio > 1:
                strengths.append("Excellent risk-adjusted returns")
            if analytics.alpha and analytics.alpha > 2:
                strengths.append("Generates alpha over benchmark")
            if analytics.maximum_drawdown and analytics.maximum_drawdown < 15:
                strengths.append("Good downside protection")
        
        return strengths if strengths else ["Performance analysis pending"]
    
    def _identify_concerns(self, analytics, returns) -> List[str]:
        """Identify areas of concern"""
        concerns = []
        
        if returns:
            if returns.return_1y and returns.return_1y < 0:
                concerns.append("Negative 1-year returns")
            if returns.return_1y and returns.return_3y and returns.return_1y < returns.return_3y / 3:
                concerns.append("Recent performance deterioration")
        
        if analytics:
            if analytics.standard_deviation and analytics.standard_deviation > 25:
                concerns.append("High volatility")
            if analytics.maximum_drawdown and analytics.maximum_drawdown > 30:
                concerns.append("High maximum drawdown")
            if analytics.sharpe_ratio and analytics.sharpe_ratio < 0:
                concerns.append("Poor risk-adjusted returns")
        
        return concerns if concerns else ["No major concerns identified"]

# Usage example
def generate_analytics_for_isin(isin: str) -> Dict:
    """
    Main function to generate analytics for a given ISIN
    
    Usage:
        analytics = generate_analytics_for_isin("INF090I01239")
        print(analytics)
    """
    generator = FundAnalyticsGenerator()
    return generator.generate_comprehensive_analytics(isin)

if __name__ == "__main__":
    # Example usage
    sample_isin = "INF090I01239"  # Replace with actual ISIN
    analytics_result = generate_analytics_for_isin(sample_isin)
    print(f"Analytics generated for {sample_isin}")
    print(f"Fund: {analytics_result.get('fund_name', 'N/A')}")
    print(f"Recommendation: {analytics_result.get('recommendation_status', {}).get('recommendation_grade', 'N/A')}")