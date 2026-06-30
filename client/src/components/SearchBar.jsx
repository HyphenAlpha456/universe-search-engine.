import React, { useState, useEffect } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import { setQuery, fetchSearchResults, clearResults } from '../features/searchSlice';
import { useDebounce } from '../hooks/useDebounce';

export default function SearchBar() {
    const dispatch = useDispatch();
    const sourceFilter = useSelector((state) => state.search.sourceFilter);
    const [localQuery, setLocalQuery] = useState('');
    const debouncedQuery = useDebounce(localQuery, 300);

    useEffect(() => {
        if (debouncedQuery) {
            dispatch(setQuery(debouncedQuery));
            dispatch(fetchSearchResults({ query: debouncedQuery, sourceFilter }));
        } else {
            dispatch(clearResults());
        }
    }, [debouncedQuery, sourceFilter, dispatch]);

    return (
        <div className="search-container">
            <input
                type="text"
                className="search-input"
                placeholder="Search the universe of developer issues..."
                value={localQuery}
                onChange={(e) => setLocalQuery(e.target.value)}
            />
        </div>
    );
}