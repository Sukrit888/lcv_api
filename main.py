import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict

# --- Configuration & Setup ---
DATA_FILE = "16 July, 2024.xlsx"
app = FastAPI(
    title="LCV Allocation API",
    description="An API to allocate LCVs to routes based on user selection and stage priority.",
    version="1.0.0"
)

# --- Data Loading ---
def load_data(file_path: str) -> pd.DataFrame:
    """Loads and preprocesses data from the Excel file."""
    try:
        df = pd.read_excel(file_path)
        df['create_date'] = pd.to_datetime(df['create_date'])
        df['date_only'] = df['create_date'].dt.date
        return df
    except FileNotFoundError:
        # In a real API, you'd use more robust logging
        print(f"Error: Data file not found at {file_path}")
        return None
    except Exception as e:
        print(f"An error occurred while loading data: {e}")
        return None

# Load data once when the API starts
df = load_data(DATA_FILE)

# --- API Data Models (for request body) ---
class AllocationRequest(BaseModel):
    selected_date: str # Format: "YYYY-MM-DD"
    selected_mgs: str
    selected_request_ids: List[str]
    lcv_stage_assignments: Dict[str, str] # e.g., {"LCV123": "Stage 3 (Filled...)"}

# --- Core Allocation Logic ---
def allocate_lcvs_to_routes_api(
    data: pd.DataFrame,
    request_ids: List[str],
    lcv_assignments: Dict[str, str]
) -> List[Dict]:
    """
    Performs a direct allocation of selected LCVs to selected routes,
    prioritizing LCVs based on their assigned stage.
    """
    # Filter the main dataframe to only include the selected routes
    selected_data = data[data['Request_id'].isin(request_ids)]
    if selected_data.empty:
        return []

    # Define the priority of each stage
    stage_priority = {
        'Stage 3 (Filled – Waiting Area/Moving to DBS)': 1,
        'Stage 2 (Filling – Safe Zone)': 2,
        'Stage 1 (Empty - Waiting Area)': 3
    }

    # Sort the selected LCVs based on the priority of their assigned stage
    selected_lcvs = list(lcv_assignments.keys())
    prioritized_lcvs = sorted(
        selected_lcvs,
        key=lambda lcv: stage_priority.get(lcv_assignments.get(lcv), 99)
    )

    # Sort the routes by duration in descending order
    sorted_routes_desc = selected_data.sort_values(by='Duration', ascending=False)
    lcvs_to_allocate = prioritized_lcvs
    allocations = []

    for _, route in sorted_routes_desc.iterrows():
        allocation_info = {
            'request_id': route['Request_id'],
            'Route_id': route['Route_id'],
            'DBS': route['DBS'],
            'Distance': route['Distance'],
            'Duration': route['Duration'],
            'Allocated_LCV': 'Pending' # Default status
        }
        if lcvs_to_allocate:
            allocation_info['Allocated_LCV'] = lcvs_to_allocate.pop(0)
        
        allocations.append(allocation_info)

    return allocations

# --- API Endpoint ---
@app.post("/allocate/", response_model=List[Dict])
async def create_allocation(request: AllocationRequest):
    """
    Receives a list of routes and LCVs with their stages, then returns
    the prioritized allocation plan.
    """
    if df is None:
        raise HTTPException(status_code=500, detail="Data could not be loaded. API is non-operational.")

    # Filter data for the specific date and MGS from the request
    try:
        request_date = pd.to_datetime(request.selected_date).date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Please use YYYY-MM-DD.")

    filtered_df = df[(df['date_only'] == request_date) & (df['MGS'] == request.selected_mgs)]

    if filtered_df.empty:
        raise HTTPException(status_code=404, detail="No data found for the selected date and MGS.")

    # Perform the allocation using the core logic
    allocation_result = allocate_lcvs_to_routes_api(
        data=filtered_df,
        request_ids=request.selected_request_ids,
        lcv_assignments=request.lcv_stage_assignments
    )

    if not allocation_result:
        raise HTTPException(status_code=404, detail="None of the selected request_ids were found in the dataset for the given date and MGS.")

    return allocation_result
