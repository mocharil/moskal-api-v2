from elasticsearch import Elasticsearch, helpers
import logging
import json
from typing import List, Dict, Any, Tuple, Set, Optional
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def upsert_documents(
    es: Elasticsearch,
    data: List[Dict[Any, Any]],
    index_name: str,
    id_field: str = 'uuid',
    fields_to_update: Optional[Set[str]] = None,
    chunk_size: int = 100
    ) -> Tuple[int, int, List[Dict]]:
    """
    Update documents if they exist, or create new ones if they don't.
    
    Args:
        es: Elasticsearch client
        data: List of documents to upsert
        index_name: Name of the index
        id_field: Field to use as document ID
        fields_to_update: Set of fields to update (if None, all fields will be updated)
        chunk_size: Number of documents per bulk request
    
    Returns:
        tuple: (updated_count, created_count, error_details)
    """
    updated_count = 0
    created_count = 0
    errors = []
    
    # First, check which documents already exist
    existing_docs = set()
    ids_to_check = [doc[id_field] for doc in data if id_field in doc]
    
    # Check existence in batches to avoid large queries
    for i in range(0, len(ids_to_check), chunk_size):
        batch_ids = ids_to_check[i:i + chunk_size]
        try:
            # Use mget to check which documents exist
            response = es.mget(index=index_name, ids=batch_ids, _source=False)
            for doc in response["docs"]:
                if doc["found"]:
                    existing_docs.add(doc["_id"])
        except Exception as e:
            logger.warning(f"Error checking document existence: {str(e)}")
    
    logger.info(f"Found {len(existing_docs)} existing documents out of {len(ids_to_check)}")
    
    # Generate actions for bulk API
    def generate_actions():
        for doc in data:
            if id_field not in doc:
                logger.warning(f"Document missing ID field {id_field}, skipping")
                continue
            
            doc_id = doc[id_field]
            
            # Prepare the document body
            if fields_to_update and doc_id in existing_docs:
                update_fields = {k: v for k, v in doc.items() if k in fields_to_update or k == id_field}
                
                if len(update_fields) <= 1:  # Only has ID field
                    logger.warning(f"Document {doc_id} has no fields to update, skipping")
                    continue
                
                doc_body = {k: v for k, v in update_fields.items()}
            else:
                doc_body = {k: v for k, v in doc.items()}
            
            if doc_id in existing_docs:
                yield {
                    "_op_type": "update",
                    "_index": index_name,
                    "_id": doc_id,
                    "doc": doc_body
                }
            else:
                yield {
                    "_op_type": "index",
                    "_index": index_name,
                    "_id": doc_id,
                    "_source": doc_body
                }
    
    # Process in chunks
    actions = list(generate_actions())
    total_actions = len(actions)
    
    logger.info(f"Preparing to upsert {total_actions} documents in {index_name}")
    
    operation_counts = {"update": 0, "index": 0}
    
    for i in range(0, total_actions, chunk_size):
        chunk = actions[i:i + chunk_size]
        chunk_num = i // chunk_size + 1
        total_chunks = (total_actions + chunk_size - 1) // chunk_size
        
        chunk_ops = {op["_op_type"]: 0 for op in chunk}
        for op in chunk:
            chunk_ops[op["_op_type"]] += 1
        
        logger.info(f"Processing chunk {chunk_num}/{total_chunks} ({len(chunk)} documents, {chunk_ops})")
        
        try:
            success, failed_items = helpers.bulk(
                es,
                chunk,
                stats_only=False,
                raise_on_error=False,
                raise_on_exception=False
            )
            
            for op in chunk:
                if op["_op_type"] in operation_counts:
                    operation_counts[op["_op_type"]] += 1
            
            if failed_items:
                for item in failed_items:
                    op_type = list(item.keys())[0]
                    item_data = item[op_type]
                    
                    if op_type in operation_counts:
                        operation_counts[op_type] -= 1
                    
                    error_info = {
                        "id": item_data.get("_id", "unknown"),
                        "index": item_data.get("_index", "unknown"),
                        "operation": op_type,
                        "error": item_data.get("error", {})
                    }
                    errors.append(error_info)
                    
                    if len(errors) <= 3:
                        logger.error(f"Operation failed: {json.dumps(error_info, indent=2)}")
                
                logger.warning(f"Chunk had {len(failed_items)} failures")
            else:
                logger.info(f"Chunk processed successfully: {success} documents")
                
        except Exception as e:
            logger.error(f"Error processing chunk: {str(e)}")
            errors.append({"chunk_error": str(e), "chunk_size": len(chunk)})
    
    updated_count = operation_counts.get("update", 0)
    created_count = operation_counts.get("index", 0)
    
    logger.info(f"Upsert complete: {updated_count} updated, {created_count} created, {len(errors)} failed")
    
    if errors:
        _summarize_errors(errors)
    
    return updated_count, created_count, errors

def _summarize_errors(errors):
    """Summarize errors by type and field"""
    error_types = {}
    for error in errors:
        if "error" in error and "type" in error["error"]:
            error_type = error["error"]["type"]
            error_types[error_type] = error_types.get(error_type, 0) + 1
        elif "chunk_error" in error:
            error_types["chunk_error"] = error_types.get("chunk_error", 0) + 1
    
    logger.error(f"Error summary by type: {json.dumps(error_types, indent=2)}")
    
    reason_pattern = {}
    for error in errors[:10]:
        if "error" in error and "reason" in error["error"]:
            reason = error["error"]["reason"]
            field_match = re.search(r'field \[(.*?)\]', reason)
            if field_match:
                field = field_match.group(1)
                reason_pattern[field] = reason_pattern.get(field, 0) + 1
    
    if reason_pattern:
        logger.error(f"Problematic fields: {json.dumps(reason_pattern, indent=2)}")
