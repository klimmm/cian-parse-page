import requests
import json
import os
import logging

logger = logging.getLogger(__name__)


def trigger_github_actions_workflow(
    repo_owner: str,
    repo_name: str,
    workflow_id: str,
    github_token: str,
    inputs: dict = None
) -> bool:
    """
    Trigger a GitHub Actions workflow via repository dispatch.
    
    Args:
        repo_owner: GitHub repository owner
        repo_name: GitHub repository name
        workflow_id: Workflow file name (e.g., 'download-images.yml')
        github_token: GitHub personal access token
        inputs: Dictionary of input parameters for the workflow
        
    Returns:
        bool: True if workflow was triggered successfully, False otherwise
    """
    if inputs is None:
        inputs = {}
    
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/actions/workflows/{workflow_id}/dispatches"
    
    headers = {
        "Authorization": f"Bearer {github_token}",
        "Accept": "application/vnd.github.v3+json",
        "Content-Type": "application/json"
    }
    
    payload = {
        "ref": "main",  # or whatever branch you want to run on
        "inputs": inputs
    }
    
    try:
        logger.info(f"🚀 Triggering GitHub Actions workflow: {workflow_id}")
        logger.info(f"📋 Inputs: {inputs}")
        
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code == 204:
            logger.info("✅ GitHub Actions workflow triggered successfully!")
            return True
        else:
            logger.error(f"❌ Failed to trigger workflow. Status: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error triggering GitHub Actions workflow: {e}")
        return False


def get_workflow_runs(
    repo_owner: str,
    repo_name: str,
    workflow_id: str,
    github_token: str,
    limit: int = 5
) -> list:
    """
    Get recent workflow runs for a specific workflow.
    
    Args:
        repo_owner: GitHub repository owner
        repo_name: GitHub repository name
        workflow_id: Workflow file name
        github_token: GitHub personal access token
        limit: Number of recent runs to retrieve
        
    Returns:
        list: List of workflow run information
    """
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/actions/workflows/{workflow_id}/runs"
    
    headers = {
        "Authorization": f"Bearer {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    params = {
        "per_page": limit
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            runs = []
            for run in data.get('workflow_runs', []):
                runs.append({
                    'id': run['id'],
                    'status': run['status'],
                    'conclusion': run['conclusion'],
                    'created_at': run['created_at'],
                    'html_url': run['html_url']
                })
            return runs
        else:
            logger.error(f"❌ Failed to get workflow runs. Status: {response.status_code}")
            return []
            
    except Exception as e:
        logger.error(f"❌ Error getting workflow runs: {e}")
        return []


def wait_for_workflow_completion(
    repo_owner: str,
    repo_name: str,
    workflow_id: str,
    github_token: str,
    timeout_minutes: int = 30
) -> bool:
    """
    Wait for the most recent workflow run to complete.
    
    Args:
        repo_owner: GitHub repository owner
        repo_name: GitHub repository name
        workflow_id: Workflow file name
        github_token: GitHub personal access token
        timeout_minutes: Maximum time to wait
        
    Returns:
        bool: True if workflow completed successfully, False otherwise
    """
    import time
    
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    
    logger.info(f"⏳ Waiting for workflow completion (timeout: {timeout_minutes} minutes)")
    
    while time.time() - start_time < timeout_seconds:
        runs = get_workflow_runs(repo_owner, repo_name, workflow_id, github_token, limit=1)
        
        if runs:
            latest_run = runs[0]
            status = latest_run['status']
            conclusion = latest_run['conclusion']
            
            logger.info(f"📊 Workflow status: {status}, conclusion: {conclusion}")
            
            if status == 'completed':
                if conclusion == 'success':
                    logger.info("✅ Workflow completed successfully!")
                    return True
                else:
                    logger.error(f"❌ Workflow completed with conclusion: {conclusion}")
                    logger.error(f"🔗 Check details: {latest_run['html_url']}")
                    return False
            
            # Still running, wait a bit
            time.sleep(30)  # Check every 30 seconds
        else:
            logger.warning("⚠️ No workflow runs found")
            time.sleep(30)
    
    logger.error(f"❌ Workflow did not complete within {timeout_minutes} minutes")
    return False