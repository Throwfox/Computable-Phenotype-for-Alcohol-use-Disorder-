import os
import re
import pandas as pd
import glob
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import pickle
import time
import gc  # For garbage collection

# Paths
keywords_path = 'keywords_regex_precise.csv'
notes_dir = '/media/volume/GLP/RDRP_6263_AUD/notes/'
results_path = 'results/'
intermediate_path = 'results/intermediate_keywords/'  # Store intermediate results

# Create intermediate directory
os.makedirs(intermediate_path, exist_ok=True)

# Load keywords and regex
print('Loading keywords and regex patterns...')
aud_keywords = pd.read_csv(keywords_path)
aud_patterns = [(re.compile(regex, re.IGNORECASE), root) 
                for root, regex in zip(aud_keywords['Root'], aud_keywords['Regex'])]

print(f"Loaded {len(aud_patterns)} AUD keyword patterns")

# Define negation patterns
negation_pattern = re.compile(
    r'\b(?:no|not|never|denies|without|negative|free of|absent|ruled out)\b', 
    re.IGNORECASE
)

# Define context filtering patterns
context_filter_pattern = re.compile(
    r'\b(?:if|recommend|suggest|advise|should|limiting|avoid|consider|encourage|'
    r'abstain|family member|family history|mother|father|parent|sibling|relative)\b', 
    re.IGNORECASE
)

# Define legal/administrative filtering patterns
legal_admin_filter_pattern = re.compile(
    r'\b(?:authorization|release|consent|information|disclosure|permission|'
    r'record|agreement|form|policy|AUDIT)\b',
    re.IGNORECASE
)

def check_sentence_for_keywords(sentence, aud_patterns, negation_pattern, 
                                  context_filter_pattern, legal_admin_filter_pattern):
    """
    Check if a sentence contains AUD keywords with negation and context filtering.
    Returns: (matched_roots, is_valid_match)
    """
    matched_roots = set()
    
    # Skip sentences with negations
    if negation_pattern.search(sentence):
        return matched_roots, False
    
    # Skip sentences with context filtering (recommendations, family history, etc.)
    if context_filter_pattern.search(sentence):
        return matched_roots, False
    
    # Skip sentences with legal/administrative content
    if legal_admin_filter_pattern.search(sentence):
        return matched_roots, False
    
    # Match AUD-related patterns
    for pattern, root in aud_patterns:
        if pattern.search(sentence):
            matched_roots.add(root)
    
    return matched_roots, len(matched_roots) > 0

def process_note_text(text, aud_patterns, negation_pattern, 
                      context_filter_pattern, legal_admin_filter_pattern):
    """
    Process note text and extract AUD-related information.
    Returns: (aud_roots, aud_roots_count, matched_sentences)
    """
    if pd.isna(text) or not text:
        return [], 0, []
    
    # Split text into sentences
    sentences = re.split(r'[.!?;:\n]+', text)
    
    all_aud_roots = set()
    matched_sentences = []
    
    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence or len(sentence) < 10:  # Skip very short sentences
            continue
        
        roots, is_valid = check_sentence_for_keywords(
            sentence, aud_patterns, negation_pattern, 
            context_filter_pattern, legal_admin_filter_pattern
        )
        
        if is_valid and roots:
            all_aud_roots.update(roots)
            matched_sentences.append(sentence)
    
    return list(all_aud_roots), len(all_aud_roots), matched_sentences

def process_single_parquet(args):
    """
    Process a single parquet file and save results immediately.
    Returns: (file_name, num_matches, num_notes, error)
    """
    file_path, file_idx, total_files = args
    file_name = os.path.basename(os.path.dirname(file_path))
    
    # Check if already processed
    output_file = os.path.join(intermediate_path, f'{file_name}_results.csv')
    if os.path.exists(output_file):
        # Don't load the parquet file for skipped files to save memory
        # Return 0 for notes count - we'll get accurate count from intermediate files later
        return file_name, 'SKIPPED', 0, None
    
    results = []
    
    try:
        # Read parquet file
        print(f"\n[{file_idx+1}/{total_files}] Processing: {file_name}")
        df = pd.read_parquet(file_path)
        total_notes = len(df)
        print(f"    └─ Loaded {total_notes:,} notes")
        
        # Process each note in the file with tqdm progress bar
        matched_count = 0
        
        # Use tqdm properly with range iteration
        for idx in tqdm(range(len(df)), 
                       desc=f"    Processing", 
                       ncols=100,
                       bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'):
            row = df.iloc[idx]
            person_id = row['OMOP_PERSON_ID']
            note_id = row['ENCOUNTER_ID']
            note_date = row['PHYSIOLOGIC_TIME']
            report_text = row['REPORT_TEXT']
            
            # Process note text
            aud_roots, aud_roots_count, matched_sentences = process_note_text(
                report_text, aud_patterns, negation_pattern,
                context_filter_pattern, legal_admin_filter_pattern
            )
            
            # Only save notes with matches
            if aud_roots_count > 0:
                matched_count += 1
                results.append({
                    'person_id': person_id,
                    'note_id': note_id,
                    'note_date': note_date,
                    'aud_roots': aud_roots,
                    'aud_roots_count': aud_roots_count,
                    'matched_sentences': matched_sentences
                })
        
        # Save intermediate results immediately
        if results:
            results_df = pd.DataFrame(results)
            results_df.to_csv(output_file, index=False)
            print(f"    └─ ✓ Found {matched_count:,} matches, saved to {file_name}_results.csv")
            
            # Clear results_df to free memory
            del results_df
        else:
            print(f"    └─ No matches found in this file")
        
        # Clear large objects to free memory
        del df
        del results
        gc.collect()  # Force garbage collection
        
        return file_name, matched_count, total_notes, None
    
    except Exception as e:
        # Clean up on error
        gc.collect()
        return file_name, 0, 0, f"Error: {e}"

# Get all parquet files
print('\nFinding all parquet files...')
parquet_files = sorted(glob.glob(f'{notes_dir}*/part-*.parquet'))
print(f"Found {len(parquet_files)} parquet files (Total ~32GB)")

# Check for existing intermediate results
existing_results = glob.glob(f'{intermediate_path}*_results.csv')
print(f"Found {len(existing_results)} previously processed files")

# Determine number of processes
n_processes = 4  # User specified
print(f"\nUsing {n_processes} parallel processes")
print(f"Intermediate results will be saved to: {intermediate_path}")
print(f"Each file will be saved immediately after processing (safe from interruption)\n")

# Prepare arguments for processing
total_files = len(parquet_files)
file_args = [(f, idx, total_files) for idx, f in enumerate(parquet_files)]

# Process files sequentially with detailed progress tracking
print("\n" + "="*80)
print("Processing parquet files...")
print("(You can safely interrupt and restart - already processed files will be skipped)")
print("="*80)

start_time = time.time()
processed_count = 0
skipped_count = 0
total_matches = 0
total_notes_processed = 0
errors = []

# Process without multiprocessing for more stable execution
for args in file_args:
    file_name, num_matches, num_notes, error = process_single_parquet(args)
    
    if error:
        errors.append(f"{file_name}: {error}")
        print(f"    └─ ✗ Error: {error}")
    elif num_matches == 'SKIPPED':
        skipped_count += 1
        # Don't add to total_notes_processed for skipped files (we don't load them)
        print(f"\n[{args[1]+1}/{total_files}] Skipped: {file_name} (already processed)")
    else:
        processed_count += 1
        total_matches += num_matches
        total_notes_processed += num_notes

elapsed_time = time.time() - start_time

# Report processing status
print(f"\n{'='*80}")
print(f"Processing Summary:")
print(f"{'='*80}")
print(f"Newly processed: {processed_count} files")
print(f"Skipped (already done): {skipped_count} files")
if processed_count > 0:
    print(f"Notes processed in this run: {total_notes_processed:,}")
    print(f"Matches found in this run: {total_matches:,}")
    print(f"Match rate: {total_matches/total_notes_processed*100:.2f}%")
    print(f"Processing speed: {total_notes_processed/elapsed_time:.1f} notes/second")
print(f"Time elapsed: {elapsed_time/60:.1f} minutes")

if errors:
    print(f"\n⚠️  Errors encountered: {len(errors)}")
    for error in errors[:5]:
        print(f"  - {error}")

# Merge all intermediate results
print(f"\n{'='*80}")
print("Merging all intermediate results...")
print(f"{'='*80}")

all_intermediate_files = glob.glob(f'{intermediate_path}*_results.csv')
print(f"Found {len(all_intermediate_files)} intermediate result files to merge")

if all_intermediate_files:
    all_results = []
    for intermediate_file in tqdm(all_intermediate_files, desc="Merging files"):
        try:
            df = pd.read_csv(intermediate_file)
            all_results.append(df)
        except Exception as e:
            print(f"Error reading {intermediate_file}: {e}")
    
    if all_results:
        # Concatenate all results
        final_df = pd.concat(all_results, ignore_index=True)
        
        # Show statistics
        print(f"\n{'='*80}")
        print("Final Statistics:")
        print(f"{'='*80}")
        print(f"Total notes with AUD keywords: {len(final_df)}")
        print(f"Total unique patients: {final_df['person_id'].nunique()}")
        print(f"Total unique notes: {final_df['note_id'].nunique()}")
        print(f"Average aud_roots per note: {final_df['aud_roots_count'].mean():.2f}")
        
        print(f"\nAUD roots distribution:")
        print(final_df['aud_roots_count'].value_counts().sort_index().head(10))
        
        # Save final merged results
        output_path = f'{results_path}aud_notes_keywords.csv'
        final_df.to_csv(output_path, index=False)
        print(f"\n✓ Final results saved to: {output_path}")
        
        # Show sample
        print(f"\nSample of results (first 3 rows):")
        print(final_df[['person_id', 'note_id', 'note_date', 'aud_roots_count']].head(3))
    else:
        print("No valid intermediate results to merge.")
else:
    print("No intermediate results found.")

print(f"\n{'='*80}")
print(f"✓ All done!")
print(f"{'='*80}")
print(f"Files processed: {processed_count + skipped_count}/{total_files}")
if processed_count > 0:
    print(f"  - Newly processed: {processed_count}")
    print(f"  - Skipped: {skipped_count}")
print(f"Intermediate files: {intermediate_path}")
print(f"Final results: {results_path}aud_notes_keywords.csv")
print(f"Total time: {elapsed_time/60:.1f} minutes")
