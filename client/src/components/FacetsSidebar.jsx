import React from 'react';
import { useDispatch, useSelector } from 'react-redux';
import { setSourceFilter } from '../features/searchSlice';

export default function FacetsSidebar() {
    const dispatch = useDispatch();
    const currentFilter = useSelector((state) => state.search.sourceFilter);

    const filters = [
        { id: 'all', label: 'All Sources' },
        { id: 'github', label: 'GitHub Issues' },
        { id: 'stackoverflow', label: 'Stack Overflow' }
    ];

    return (
        <aside className="sidebar">
            <h2 className="sidebar-title">Filters</h2>
            <div className="filter-group">
                {filters.map((filter) => (
                    <button
                        key={filter.id}
                        className={`filter-btn ${currentFilter === filter.id ? 'active' : ''}`}
                        onClick={() => dispatch(setSourceFilter(filter.id))}
                    >
                        {filter.label}
                    </button>
                ))}
            </div>
        </aside>
    );
}