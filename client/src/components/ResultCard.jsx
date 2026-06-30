import React from 'react';

export default function ResultCard({ issue }) {
    const isGitHub = issue.source === 'github';

    return (
        <a href={issue.url} target="_blank" rel="noopener noreferrer" className="result-card">
            <div className="card-header">
                <span className={`badge ${isGitHub ? 'badge-github' : 'badge-so'}`}>
                    {isGitHub ? 'GitHub' : 'Stack Overflow'}
                </span>
                <h3 className="card-title">{issue.title}</h3>
            </div>
            <p className="card-body">
                {issue.body.length > 200 ? issue.body.substring(0, 200) + '...' : issue.body}
            </p>
        </a>
    );
}