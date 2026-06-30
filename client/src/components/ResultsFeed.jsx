import React from 'react';
import { useSelector } from 'react-redux';
import ResultCard from './ResultCard';

export default function ResultsFeed() {
    const { results, status, query } = useSelector((state) => state.search);

    if (status === 'loading') {
        return (
            <div className="feed-container">
                <div className="loader">Searching universe...</div>
            </div>
        );
    }

    if (status === 'succeeded' && results.length === 0 && query) {
        return (
            <div className="feed-container">
                <div className="empty-state">No solutions found for "{query}".</div>
            </div>
        );
    }

    return (
        <div className="feed-container">
            {results.map((issue) => (
                <ResultCard key={issue.id} issue={issue} />
            ))}
        </div>
    );
}