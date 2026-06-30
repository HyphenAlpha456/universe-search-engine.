import React from 'react';
import SearchBar from './components/SearchBar';
import FacetsSidebar from './components/FacetsSidebar';
import ResultsFeed from './components/ResultsFeed';
import './index.css';

function App() {
    return (
        <div className="app-layout">
            <header className="app-header">
                <h1>Universe Search Engine</h1>
                <SearchBar />
            </header>
            <main className="app-main">
                <FacetsSidebar />
                <ResultsFeed />
            </main>
        </div>
    );
}

export default App;