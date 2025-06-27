"""
Portfolio Overlap Analyzer for Mutual Funds
Analyzes portfolio overlaps between multiple funds given a list of ISINs
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
from datetime import datetime

from setup_db import db
from models import Fund, FundHolding, FundStatistics

logger = logging.getLogger(__name__)

class PortfolioOverlapAnalyzer:
    """
    Analyze portfolio overlaps between multiple mutual funds
    """
    
    def __init__(self):
        """Initialize the portfolio overlap analyzer"""
        pass
    
    def analyze_portfolio_overlap(self, fund_isins: List[str]) -> Dict:
        """
        Analyze portfolio overlap between multiple funds
        
        Args:
            fund_isins (List[str]): List of fund ISINs to analyze
            
        Returns:
            Dict: Comprehensive overlap analysis
        """
        logger.info(f"Analyzing portfolio overlap for {len(fund_isins)} funds")
        
        if len(fund_isins) < 2:
            return {"error": "At least 2 funds required for overlap analysis"}
        
        # Validate all funds exist
        funds_data = self._get_funds_data(fund_isins)
        if not funds_data:
            return {"error": "No valid funds found"}
        
        # Get portfolio holdings for all funds
        all_holdings = self._get_all_holdings(fund_isins)
        if not all_holdings:
            return {"error": "No portfolio holdings data available"}
        
        overlap_analysis = {
            "analysis_date": datetime.utcnow().isoformat(),
            "funds_analyzed": len(fund_isins),
            "fund_details": funds_data,
            "overall_overlap": self._calculate_overall_overlap(all_holdings),
            "pairwise_overlap": self._calculate_pairwise_overlap(all_holdings),
            "common_holdings": self._identify_common_holdings(all_holdings),
            "sector_overlap": self._analyze_sector_overlap(all_holdings),
            "overlap_matrix": self._create_overlap_matrix(all_holdings),
            "diversification_score": self._calculate_diversification_score(all_holdings),
            "recommendations": self._generate_overlap_recommendations(all_holdings, funds_data)
        }
        
        return overlap_analysis
    
    def _get_funds_data(self, fund_isins: List[str]) -> List[Dict]:
        """Get basic fund information"""
        funds_data = []
        
        for isin in fund_isins:
            fund = Fund.query.filter_by(isin=isin).first()
            if fund:
                funds_data.append({
                    "isin": isin,
                    "name": fund.scheme_name,
                    "amc": fund.amc_name,
                    "type": fund.fund_type,
                    "subtype": fund.fund_subtype
                })
            else:
                logger.warning(f"Fund with ISIN {isin} not found")
        
        return funds_data
    
    def _get_all_holdings(self, fund_isins: List[str]) -> Dict[str, List[Dict]]:
        """Get holdings data for all funds"""
        all_holdings = {}
        
        for isin in fund_isins:
            holdings = FundHolding.query.filter_by(isin=isin).all()
            if holdings:
                all_holdings[isin] = [{
                    "instrument_name": h.instrument_name,
                    "instrument_isin": h.instrument_isin,
                    "sector": h.sector,
                    "percentage": h.percentage_to_nav,
                    "value": h.value,
                    "instrument_type": h.instrument_type
                } for h in holdings]
            else:
                logger.warning(f"No holdings found for ISIN {isin}")
                all_holdings[isin] = []
        
        return all_holdings
    
    def _calculate_overall_overlap(self, all_holdings: Dict[str, List[Dict]]) -> Dict:
        """Calculate overall portfolio overlap statistics"""
        fund_isins = list(all_holdings.keys())
        
        if len(fund_isins) < 2:
            return {"error": "Insufficient funds for overlap calculation"}
        
        # Create instrument-to-funds mapping
        instrument_funds = defaultdict(list)
        total_instruments = set()
        
        for isin, holdings in all_holdings.items():
            fund_instruments = set()
            for holding in holdings:
                instrument_key = self._create_instrument_key(holding)
                if instrument_key:
                    instrument_funds[instrument_key].append(isin)
                    fund_instruments.add(instrument_key)
                    total_instruments.add(instrument_key)
            
        # Calculate overlap statistics
        common_instruments = {k: v for k, v in instrument_funds.items() if len(v) > 1}
        
        overlap_stats = {
            "total_unique_instruments": len(total_instruments),
            "common_instruments_count": len(common_instruments),
            "overlap_percentage": (len(common_instruments) / len(total_instruments) * 100) if total_instruments else 0,
            "funds_with_most_overlap": self._find_most_overlapping_funds(instrument_funds),
            "overlap_distribution": self._calculate_overlap_distribution(instrument_funds, len(fund_isins))
        }
        
        return overlap_stats
    
    def _calculate_pairwise_overlap(self, all_holdings: Dict[str, List[Dict]]) -> List[Dict]:
        """Calculate pairwise overlap between all fund combinations"""
        fund_isins = list(all_holdings.keys())
        pairwise_overlaps = []
        
        for i in range(len(fund_isins)):
            for j in range(i + 1, len(fund_isins)):
                isin1, isin2 = fund_isins[i], fund_isins[j]
                overlap = self._calculate_two_fund_overlap(
                    all_holdings[isin1], 
                    all_holdings[isin2], 
                    isin1, 
                    isin2
                )
                pairwise_overlaps.append(overlap)
        
        return pairwise_overlaps
    
    def _calculate_two_fund_overlap(self, holdings1: List[Dict], holdings2: List[Dict], 
                                  isin1: str, isin2: str) -> Dict:
        """Calculate overlap between two specific funds"""
        # Create instrument dictionaries
        instruments1 = {self._create_instrument_key(h): h for h in holdings1 if self._create_instrument_key(h)}
        instruments2 = {self._create_instrument_key(h): h for h in holdings2 if self._create_instrument_key(h)}
        
        # Find common instruments
        common_keys = set(instruments1.keys()) & set(instruments2.keys())
        
        # Calculate overlap metrics
        overlap_by_count = len(common_keys) / max(len(instruments1), len(instruments2)) * 100 if instruments1 or instruments2 else 0
        
        # Calculate overlap by weight
        total_overlap_weight = 0
        common_holdings_detail = []
        
        for key in common_keys:
            holding1 = instruments1[key]
            holding2 = instruments2[key]
            min_weight = min(holding1["percentage"], holding2["percentage"])
            total_overlap_weight += min_weight
            
            common_holdings_detail.append({
                "instrument_name": holding1["instrument_name"],
                "instrument_isin": holding1.get("instrument_isin"),
                "sector": holding1.get("sector"),
                "fund1_weight": holding1["percentage"],
                "fund2_weight": holding2["percentage"],
                "overlap_weight": min_weight
            })
        
        # Sort by overlap weight
        common_holdings_detail.sort(key=lambda x: x["overlap_weight"], reverse=True)
        
        return {
            "fund1_isin": isin1,
            "fund2_isin": isin2,
            "overlap_by_count_percentage": overlap_by_count,
            "overlap_by_weight_percentage": total_overlap_weight,
            "common_holdings_count": len(common_keys),
            "fund1_total_holdings": len(instruments1),
            "fund2_total_holdings": len(instruments2),
            "common_holdings": common_holdings_detail[:20],  # Top 20 overlapping holdings
            "overlap_grade": self._assign_overlap_grade(overlap_by_count, total_overlap_weight)
        }
    
    def _identify_common_holdings(self, all_holdings: Dict[str, List[Dict]]) -> Dict:
        """Identify holdings common across multiple funds"""
        instrument_funds = defaultdict(list)
        instrument_details = {}
        
        # Build instrument mapping
        for isin, holdings in all_holdings.items():
            for holding in holdings:
                instrument_key = self._create_instrument_key(holding)
                if instrument_key:
                    instrument_funds[instrument_key].append({
                        "fund_isin": isin,
                        "weight": holding["percentage"],
                        "value": holding.get("value")
                    })
                    if instrument_key not in instrument_details:
                        instrument_details[instrument_key] = {
                            "name": holding["instrument_name"],
                            "isin": holding.get("instrument_isin"),
                            "sector": holding.get("sector"),
                            "type": holding.get("instrument_type")
                        }
        
        # Filter for common holdings (present in 2+ funds)
        common_holdings = {}
        for instrument_key, fund_list in instrument_funds.items():
            if len(fund_list) >= 2:
                total_weight = sum(f["weight"] for f in fund_list)
                avg_weight = total_weight / len(fund_list)
                
                common_holdings[instrument_key] = {
                    "details": instrument_details[instrument_key],
                    "present_in_funds": len(fund_list),
                    "fund_weights": fund_list,
                    "average_weight": avg_weight,
                    "total_weight": total_weight,
                    "weight_variance": np.var([f["weight"] for f in fund_list])
                }
        
        # Sort by number of funds and average weight
        sorted_common = sorted(
            common_holdings.items(), 
            key=lambda x: (x[1]["present_in_funds"], x[1]["average_weight"]), 
            reverse=True
        )
        
        return {
            "total_common_holdings": len(common_holdings),
            "holdings_in_all_funds": len([h for h in common_holdings.values() if h["present_in_funds"] == len(all_holdings)]),
            "top_common_holdings": dict(sorted_common[:50]),  # Top 50 common holdings
            "common_by_fund_count": self._group_common_by_fund_count(common_holdings, len(all_holdings))
        }
    
    def _analyze_sector_overlap(self, all_holdings: Dict[str, List[Dict]]) -> Dict:
        """Analyze sector-wise overlap between funds"""
        fund_sectors = {}
        all_sectors = set()
        
        # Calculate sector allocation for each fund
        for isin, holdings in all_holdings.items():
            sector_allocation = defaultdict(float)
            total_allocation = 0
            
            for holding in holdings:
                if holding.get("sector"):
                    sector_allocation[holding["sector"]] += holding["percentage"]
                    total_allocation += holding["percentage"]
                    all_sectors.add(holding["sector"])
            
            fund_sectors[isin] = dict(sector_allocation)
        
        # Calculate sector overlap matrix
        sector_overlap_matrix = {}
        fund_isins = list(fund_sectors.keys())
        
        for i in range(len(fund_isins)):
            for j in range(i + 1, len(fund_isins)):
                isin1, isin2 = fund_isins[i], fund_isins[j]
                overlap = self._calculate_sector_overlap_between_funds(
                    fund_sectors[isin1], 
                    fund_sectors[isin2]
                )
                pair_key = f"{isin1}-{isin2}"
                sector_overlap_matrix[pair_key] = overlap
        
        return {
            "total_sectors": len(all_sectors),
            "sector_list": list(all_sectors),
            "fund_sector_allocations": fund_sectors,
            "sector_overlap_matrix": sector_overlap_matrix,
            "average_sector_overlap": np.mean([v["overlap_percentage"] for v in sector_overlap_matrix.values()]) if sector_overlap_matrix else 0
        }
    
    def _create_overlap_matrix(self, all_holdings: Dict[str, List[Dict]]) -> Dict:
        """Create a matrix showing overlap percentages between all fund pairs"""
        fund_isins = list(all_holdings.keys())
        matrix = {}
        
        for isin1 in fund_isins:
            matrix[isin1] = {}
            for isin2 in fund_isins:
                if isin1 == isin2:
                    matrix[isin1][isin2] = 100.0  # Perfect overlap with itself
                else:
                    # Find if we already calculated this pair
                    overlap_data = None
                    for pair in self._calculate_pairwise_overlap(all_holdings):
                        if ((pair["fund1_isin"] == isin1 and pair["fund2_isin"] == isin2) or
                            (pair["fund1_isin"] == isin2 and pair["fund2_isin"] == isin1)):
                            overlap_data = pair
                            break
                    
                    if overlap_data:
                        matrix[isin1][isin2] = overlap_data["overlap_by_weight_percentage"]
                    else:
                        # Calculate on the fly if not found
                        overlap = self._calculate_two_fund_overlap(
                            all_holdings[isin1], all_holdings[isin2], isin1, isin2
                        )
                        matrix[isin1][isin2] = overlap["overlap_by_weight_percentage"]
        
        return {
            "matrix": matrix,
            "fund_order": fund_isins,
            "average_overlap": self._calculate_average_matrix_overlap(matrix)
        }
    
    def _calculate_diversification_score(self, all_holdings: Dict[str, List[Dict]]) -> Dict:
        """Calculate overall diversification score for the portfolio combination"""
        if len(all_holdings) < 2:
            return {"error": "Need at least 2 funds for diversification analysis"}
        
        # Calculate overlap penalty
        pairwise_overlaps = self._calculate_pairwise_overlap(all_holdings)
        avg_overlap = np.mean([p["overlap_by_weight_percentage"] for p in pairwise_overlaps])
        
        # Calculate sector diversification
        all_sectors = set()
        total_holdings = 0
        
        for holdings in all_holdings.values():
            sectors_in_fund = set(h.get("sector") for h in holdings if h.get("sector"))
            all_sectors.update(sectors_in_fund)
            total_holdings += len(holdings)
        
        # Diversification scoring
        overlap_penalty = min(50, avg_overlap)  # Max 50 point penalty
        sector_bonus = min(30, len(all_sectors) * 2)  # Max 30 point bonus
        holding_bonus = min(20, total_holdings / 50)  # Max 20 point bonus
        
        diversification_score = 100 - overlap_penalty + sector_bonus + holding_bonus
        diversification_score = max(0, min(100, diversification_score))
        
        return {
            "diversification_score": round(diversification_score, 2),
            "score_grade": self._assign_diversification_grade(diversification_score),
            "score_components": {
                "base_score": 100,
                "overlap_penalty": -overlap_penalty,
                "sector_bonus": sector_bonus,
                "holding_bonus": holding_bonus
            },
            "metrics": {
                "average_overlap_percentage": round(avg_overlap, 2),
                "total_sectors": len(all_sectors),
                "total_unique_holdings": total_holdings
            }
        }
    
    def _generate_overlap_recommendations(self, all_holdings: Dict[str, List[Dict]], 
                                        funds_data: List[Dict]) -> List[str]:
        """Generate recommendations based on overlap analysis"""
        recommendations = []
        
        # Calculate overall metrics
        pairwise_overlaps = self._calculate_pairwise_overlap(all_holdings)
        avg_overlap = np.mean([p["overlap_by_weight_percentage"] for p in pairwise_overlaps])
        
        # High overlap warning
        if avg_overlap > 40:
            recommendations.append("HIGH OVERLAP WARNING: Average portfolio overlap exceeds 40%. Consider reducing fund count or choosing funds from different categories.")
        
        # Identify most overlapping pairs
        high_overlap_pairs = [p for p in pairwise_overlaps if p["overlap_by_weight_percentage"] > 60]
        if high_overlap_pairs:
            recommendations.append(f"Found {len(high_overlap_pairs)} fund pairs with >60% overlap. Consider replacing one fund from each highly overlapping pair.")
        
        # Sector concentration check
        sector_analysis = self._analyze_sector_overlap(all_holdings)
        if sector_analysis["average_sector_overlap"] > 50:
            recommendations.append("High sector overlap detected. Consider adding funds from different sectors for better diversification.")
        
        # Fund type diversity check
        fund_types = set(f["type"] for f in funds_data)
        if len(fund_types) == 1:
            recommendations.append("All funds are from the same category. Consider mixing equity, debt, and hybrid funds for better diversification.")
        
        # AMC concentration check
        amc_count = len(set(f["amc"] for f in funds_data))
        if amc_count < len(funds_data) / 2:
            recommendations.append("High AMC concentration detected. Consider spreading investments across different fund houses.")
        
        # Positive recommendations
        if avg_overlap < 25:
            recommendations.append("GOOD DIVERSIFICATION: Portfolio shows good diversification with acceptable overlap levels.")
        
        if len(all_holdings) >= 3 and avg_overlap < 35:
            recommendations.append("Well-balanced portfolio with multiple funds showing reasonable overlap.")
        
        return recommendations if recommendations else ["Portfolio analysis complete. No specific recommendations at this time."]
    
    # Helper methods
    def _create_instrument_key(self, holding: Dict) -> Optional[str]:
        """Create a unique key for instrument identification"""
        # Prefer ISIN, fallback to name
        if holding.get("instrument_isin") and holding["instrument_isin"] != "-":
            return f"ISIN:{holding['instrument_isin']}"
        elif holding.get("instrument_name"):
            # Clean up name for better matching
            name = holding["instrument_name"].strip().upper()
            return f"NAME:{name}"
        return None
    
    def _find_most_overlapping_funds(self, instrument_funds: Dict) -> List[Dict]:
        """Find funds with highest overlap"""
        fund_overlap_count = defaultdict(int)
        
        for funds_list in instrument_funds.values():
            if len(funds_list) > 1:
                for fund in funds_list:
                    fund_overlap_count[fund] += 1
        
        sorted_funds = sorted(fund_overlap_count.items(), key=lambda x: x[1], reverse=True)
        return [{"fund_isin": f, "overlap_instruments": count} for f, count in sorted_funds[:5]]
    
    def _calculate_overlap_distribution(self, instrument_funds: Dict, total_funds: int) -> Dict:
        """Calculate distribution of instruments by number of funds"""
        distribution = defaultdict(int)
        
        for funds_list in instrument_funds.values():
            fund_count = len(funds_list)
            distribution[fund_count] += 1
        
        return {f"in_{i}_funds": distribution[i] for i in range(1, total_funds + 1)}
    
    def _assign_overlap_grade(self, count_overlap: float, weight_overlap: float) -> str:
        """Assign overlap grade based on overlap percentages"""
        avg_overlap = (count_overlap + weight_overlap) / 2
        
        if avg_overlap >= 80:
            return "Very High Overlap"
        elif avg_overlap >= 60:
            return "High Overlap"
        elif avg_overlap >= 40:
            return "Moderate Overlap"
        elif avg_overlap >= 20:
            return "Low Overlap"
        else:
            return "Minimal Overlap"
    
    def _group_common_by_fund_count(self, common_holdings: Dict, total_funds: int) -> Dict:
        """Group common holdings by number of funds they appear in"""
        grouped = defaultdict(list)
        
        for holding_key, holding_data in common_holdings.items():
            fund_count = holding_data["present_in_funds"]
            grouped[fund_count].append({
                "key": holding_key,
                "name": holding_data["details"]["name"],
                "average_weight": holding_data["average_weight"]
            })
        
        # Sort within each group by average weight
        for fund_count in grouped:
            grouped[fund_count].sort(key=lambda x: x["average_weight"], reverse=True)
        
        return dict(grouped)
    
    def _calculate_sector_overlap_between_funds(self, sectors1: Dict, sectors2: Dict) -> Dict:
        """Calculate sector overlap between two funds"""
        all_sectors = set(sectors1.keys()) | set(sectors2.keys())
        
        total_overlap = 0
        sector_details = {}
        
        for sector in all_sectors:
            weight1 = sectors1.get(sector, 0)
            weight2 = sectors2.get(sector, 0)
            overlap = min(weight1, weight2)
            total_overlap += overlap
            
            if weight1 > 0 or weight2 > 0:
                sector_details[sector] = {
                    "fund1_weight": weight1,
                    "fund2_weight": weight2,
                    "overlap": overlap
                }
        
        return {
            "overlap_percentage": total_overlap,
            "sector_details": sector_details,
            "common_sectors": len([s for s in all_sectors if sectors1.get(s, 0) > 0 and sectors2.get(s, 0) > 0])
        }
    
    def _calculate_average_matrix_overlap(self, matrix: Dict) -> float:
        """Calculate average overlap from matrix (excluding diagonal)"""
        overlaps = []
        for isin1, row in matrix.items():
            for isin2, overlap in row.items():
                if isin1 != isin2:  # Exclude diagonal
                    overlaps.append(overlap)
        
        return np.mean(overlaps) if overlaps else 0
    
    def _assign_diversification_grade(self, score: float) -> str:
        """Assign diversification grade based on score"""
        if score >= 80:
            return "Excellent Diversification"
        elif score >= 65:
            return "Good Diversification"
        elif score >= 50:
            return "Moderate Diversification"
        elif score >= 35:
            return "Poor Diversification"
        else:
            return "Very Poor Diversification"

# Usage functions
def analyze_fund_overlap(fund_isins: List[str]) -> Dict:
    """
    Main function to analyze portfolio overlap for a list of funds
    
    Args:
        fund_isins: List of fund ISINs to analyze
        
    Returns:
        Dict: Comprehensive overlap analysis
        
    Usage:
        overlap_analysis = analyze_fund_overlap([
            "INF090I01239", 
            "INF090I01247", 
            "INF090I01255"
        ])
    """
    analyzer = PortfolioOverlapAnalyzer()
    return analyzer.analyze_portfolio_overlap(fund_isins)

def get_overlap_summary(fund_isins: List[str]) -> Dict:
    """
    Get a simplified overlap summary for quick analysis
    
    Args:
        fund_isins: List of fund ISINs
        
    Returns:
        Dict: Simplified overlap summary
    """
    full_analysis = analyze_fund_overlap(fund_isins)
    
    if "error" in full_analysis:
        return full_analysis
    
    return {
        "funds_count": full_analysis["funds_analyzed"],
        "average_overlap": full_analysis["overall_overlap"]["overlap_percentage"],
        "diversification_score": full_analysis["diversification_score"]["diversification_score"],
        "diversification_grade": full_analysis["diversification_score"]["score_grade"],
        "top_recommendation": full_analysis["recommendations"][0] if full_analysis["recommendations"] else "No recommendations",
        "high_overlap_pairs": len([p for p in full_analysis["pairwise_overlap"] if p["overlap_by_weight_percentage"] > 50])
    }

if __name__ == "__main__":
    # Example usage
    sample_funds = ["INF090I01239", "INF090I01247", "INF090I01255"]  # Replace with actual ISINs
    
    print("=== Portfolio Overlap Analysis ===")
    overlap_result = analyze_fund_overlap(sample_funds)
    
    if "error" not in overlap_result:
        print(f"Funds analyzed: {overlap_result['funds_analyzed']}")
        print(f"Average overlap: {overlap_result['overall_overlap']['overlap_percentage']:.2f}%")
        print(f"Diversification score: {overlap_result['diversification_score']['diversification_score']:.2f}")
        print(f"Grade: {overlap_result['diversification_score']['score_grade']}")
        print("\nTop recommendations:")
        for rec in overlap_result['recommendations'][:3]:
            print(f"- {rec}")
    else:
        print(f"Error: {overlap_result['error']}")