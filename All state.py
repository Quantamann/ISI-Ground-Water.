import pandas as pd
import os
from pathlib import Path

def is_file_usable(file_path, station_col_name="Station_name", level_col_name="level"):
    """Check if a CSV file contains usable data."""
    try:
        df = pd.read_csv(file_path)
        
        if len(df) == 0:
            return False
        
        if len(df) == 1 and 'No Data Available' in str(df.values):
            return False
        
        df.columns = df.columns.str.strip()
        
        if level_col_name not in df.columns:
            possible_level_cols = [col for col in df.columns if 'level' in col.lower()]
            if not possible_level_cols:
                return False
            level_col_name = possible_level_cols[0]
        
        required_cols = ['Date', 'State', 'District', station_col_name, level_col_name]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False
        
        return True
    except:
        return False


def process_single_state(state_folder, station_col_name="Station_name", level_col_name="level"):
    """Process all CSV files from a single state folder."""
    csv_files = sorted([f for f in os.listdir(state_folder) if f.endswith('.csv')])
    
    if not csv_files:
        print(f"  ✗ No CSV files found in {state_folder}")
        return None
    
    state_name = os.path.basename(state_folder).split('_')[0]
    print(f"\n{'='*60}")
    print(f"Processing State: {state_name}")
    print(f"Found {len(csv_files)} CSV files")
    
    # Check file quality
    print("  Checking file quality...")
    usable_count = 0
    for csv_file in csv_files:
        file_path = os.path.join(state_folder, csv_file)
        if is_file_usable(file_path, station_col_name, level_col_name):
            usable_count += 1
    
    usable_ratio = usable_count / len(csv_files)
    unusable_count = len(csv_files) - usable_count
    
    print(f"  Usable files: {usable_count}/{len(csv_files)} ({usable_ratio*100:.1f}%)")
    print(f"  Unusable files: {unusable_count}/{len(csv_files)} ({(1-usable_ratio)*100:.1f}%)")
    
    if usable_count == 0:
        print(f"  ✗ SKIPPING {state_name}: No usable files found")
        print('='*60)
        return None
    
    print('='*60)
    
    all_pivoted_dfs = []
    processed_count = 0
    
    for csv_file in csv_files:
        try:
            file_path = os.path.join(state_folder, csv_file)
            
            if not is_file_usable(file_path, station_col_name, level_col_name):
                continue
            
            if processed_count % 50 == 0 or processed_count == 0:
                print(f"  Processing [{processed_count + 1}/{usable_count}]: {csv_file}")
            
            df = pd.read_csv(file_path)
            df.columns = df.columns.str.strip()
            
            actual_level_col = level_col_name
            if level_col_name not in df.columns:
                possible_level_cols = [col for col in df.columns if 'level' in col.lower()]
                if possible_level_cols:
                    actual_level_col = possible_level_cols[0]
            
            pivot_df = df.pivot_table(
                index='Date',
                columns=['State', 'District', station_col_name],
                values=actual_level_col,
                aggfunc='first'
            )
            
            pivot_df = pivot_df.reset_index()
            pivot_df.columns.name = None
            
            new_cols = [('Date', '', '')] + [(col[0], col[1], col[2]) for col in pivot_df.columns[1:]]
            pivot_df.columns = pd.MultiIndex.from_tuples(new_cols)
            
            all_pivoted_dfs.append(pivot_df)
            processed_count += 1
            
        except Exception as e:
            if processed_count % 50 == 0:
                print(f"  ✗ Error: {csv_file}: {str(e)}")
            continue
    
    if not all_pivoted_dfs:
        print(f"  ✗ No files successfully processed for {state_name}")
        return None
    
    state_df = pd.concat(all_pivoted_dfs, axis=0, ignore_index=True)
    state_df = state_df.sort_values(by=('Date', '', ''))
    
    print(f"  ✓ {state_name}: {len(all_pivoted_dfs)} files processed")
    print(f"  ✓ Total rows: {state_df.shape[0]}")
    print(f"  ✓ Total stations: {state_df.shape[1] - 1}")
    
    return state_df


def process_all_states_horizontal(parent_folder, station_col_name="Station_name", 
                                   level_col_name="level", output_file="all_states_horizontal.csv"):
    """Process all state folders and stack them horizontally."""
    
    state_folders = [
        os.path.join(parent_folder, folder) 
        for folder in os.listdir(parent_folder) 
        if os.path.isdir(os.path.join(parent_folder, folder)) and 'groundWater' in folder
    ]
    
    state_folders = sorted(state_folders)
    
    if not state_folders:
        print("No state folders found!")
        return None
    
    print(f"\n{'#'*60}")
    print(f"Found {len(state_folders)} state folders to process")
    print(f"All states will be included (regardless of unusable file ratio)")
    print(f"{'#'*60}")
    
    state_dataframes = {}
    skipped_states = []
    
    for state_folder in state_folders:
        state_name = os.path.basename(state_folder).split('_')[0]
        state_df = process_single_state(state_folder, station_col_name, level_col_name)
        
        if state_df is not None:
            state_df_flat = state_df.copy()
            date_values = state_df_flat[('Date', '', '')].values
            state_df_flat = state_df_flat.drop(columns=[('Date', '', '')])
            state_df_flat.insert(0, 'Date', date_values)
            state_dataframes[state_name] = state_df_flat
        else:
            skipped_states.append(state_name)
    
    if not state_dataframes:
        print("\n✗ No states were successfully processed!")
        return None
    
    print(f"\n{'#'*60}")
    print(f"Successfully processed: {len(state_dataframes)} states")
    if skipped_states:
        print(f"Skipped (no usable files): {len(skipped_states)} states")
        print(f"Skipped: {', '.join(skipped_states)}")
    print(f"\nMerging {len(state_dataframes)} states horizontally...")
    print(f"{'#'*60}")
    
    states_list = list(state_dataframes.keys())
    combined_df = state_dataframes[states_list[0]]
    print(f"Starting with: {states_list[0]} ({combined_df.shape[1]-1} stations)")
    
    for state_name in states_list[1:]:
        state_df = state_dataframes[state_name]
        print(f"Merging: {state_name} ({state_df.shape[1]-1} stations)")
        
        combined_df = pd.merge(
            combined_df,
            state_df,
            on='Date',
            how='outer',
            suffixes=('', f'_{state_name}')
        )
    
    combined_df = combined_df.sort_values(by='Date').reset_index(drop=True)
    combined_df.to_csv(output_file, index=False)
    
    print(f"\n{'='*60}")
    print(f"✓ FINAL OUTPUT SAVED TO: {output_file}")
    print(f"{'='*60}")
    print(f"✓ Total rows (dates): {combined_df.shape[0]}")
    print(f"✓ Total columns: {combined_df.shape[1]} (1 Date + {combined_df.shape[1]-1} stations)")
    print(f"✓ States included: {len(states_list)}")
    print(f"✓ Date range: {combined_df['Date'].min()} to {combined_df['Date'].max()}")
    
    print(f"\n✓ States processed:")
    for state_name, df in state_dataframes.items():
        print(f"  - {state_name}: {df.shape[1]-1} stations")
    
    if skipped_states:
        print(f"\n✗ States skipped (no usable files found):")
        for state_name in skipped_states:
            print(f"  - {state_name}")
    
    return combined_df


if __name__ == "__main__":
    parent_folder = "/Users/soumalya/Downloads/Source."
    
    result = process_all_states_horizontal(
        parent_folder=parent_folder,
        station_col_name="Station_name",
        level_col_name="level",
        output_file="india_all_states_groundwater4.csv"
    )
    
    if result is not None:
        print("\n" + "="*60)
        print("PREVIEW OF FINAL DATA:")
        print("="*60)
        print(f"Shape: {result.shape}")
        print(f"\nFirst 5 rows, first 10 columns:")
        print(result.iloc[:5, :10])


rows,columns=combined_df.shape()
print("rows","columns")




