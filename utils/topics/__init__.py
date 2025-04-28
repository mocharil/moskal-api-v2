"""
Topic Analysis Package

This package provides functionality for analyzing and clustering social media topics,
with support for both new and existing projects.

Main Components:
- ES Operations: Handles Elasticsearch document operations
- Topic Analyzer: Implements core topic analysis and clustering logic
- Topic Manager: Coordinates the overall topic analysis workflow

Main Interface:
    topic_overviews(): Primary function for analyzing topics
"""

from .topic_manager import topic_overviews

__all__ = ['topic_overviews']
