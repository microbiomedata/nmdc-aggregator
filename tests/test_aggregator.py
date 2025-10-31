"""Tests for the Aggregator class"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from aggregator import Aggregator


class ConcreteAggregator(Aggregator):
    """Concrete implementation of Aggregator for testing"""
    
    def __init__(self):
        # Skip the parent's __init__ to avoid automatic get_bearer_token() call
        # Set the necessary attributes manually
        self.base_url = "https://api-dev.microbiomedata.org"
        self.aggregation_filter = ""
        self.workflow_filter = ""
    
    def generate_aggregations(self):
        """Stub implementation"""
        pass
    
    def process_activity(self, act):
        """Stub implementation of abstract method"""
        pass


class TestGetBearerToken:
    """Tests for get_bearer_token method"""
    
    @patch('aggregator.requests.post')
    def test_get_bearer_token_success(self, mock_post):
        """Test that get_bearer_token works correctly with 200 response"""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access_token": "test_token_123"}
        mock_post.return_value = mock_response
        
        # Create aggregator and get token
        agg = ConcreteAggregator()
        agg.get_bearer_token()
        
        # Verify token was set
        assert agg.nmdc_api_token == "test_token_123"
        # Verify json() was called
        assert mock_response.json.call_count == 1
    
    @patch('aggregator.requests.post')
    def test_get_bearer_token_non_200_status(self, mock_post):
        """Test that get_bearer_token raises exception for non-200 status codes"""
        # Mock failed response with 401 status
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_post.return_value = mock_response
        
        # Create aggregator and attempt to get token
        agg = ConcreteAggregator()
        
        # Verify exception is raised and json() is NOT called
        with pytest.raises(Exception) as exc_info:
            agg.get_bearer_token()
        
        assert "Getting token failed with status code: 401" in str(exc_info.value)
        assert "Unauthorized" in str(exc_info.value)
        # Verify json() was NOT called when status code is not 200
        mock_response.json.assert_not_called()
    
    @patch('aggregator.requests.post')
    def test_get_bearer_token_500_status(self, mock_post):
        """Test that get_bearer_token raises exception for 500 server error"""
        # Mock server error response
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_post.return_value = mock_response
        
        # Create aggregator and attempt to get token
        agg = ConcreteAggregator()
        
        # Verify exception is raised
        with pytest.raises(Exception) as exc_info:
            agg.get_bearer_token()
        
        assert "Getting token failed with status code: 500" in str(exc_info.value)
        # Verify json() was NOT called
        mock_response.json.assert_not_called()
    
    @patch('aggregator.requests.post')
    def test_get_bearer_token_missing_access_token(self, mock_post):
        """Test that get_bearer_token raises exception when access_token is missing from response"""
        # Mock response with 200 but no access_token
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"error": "invalid_client"}
        mock_post.return_value = mock_response
        
        # Create aggregator and attempt to get token
        agg = ConcreteAggregator()
        
        # Verify exception is raised
        with pytest.raises(Exception) as exc_info:
            agg.get_bearer_token()
        
        assert "Getting token failed" in str(exc_info.value)
        # Verify json() WAS called since status code was 200
        assert mock_response.json.call_count == 1
