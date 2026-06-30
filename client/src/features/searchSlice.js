import { createSlice, createAsyncThunk } from '@reduxjs/toolkit';
import axios from 'axios';

export const fetchSearchResults = createAsyncThunk(
    'search/fetchResults',
    async ({ query, sourceFilter }) => {
        if (!query) return [];
        const response = await axios.get('http://localhost:5000/api/search', {
            params: { q: query, source: sourceFilter }
        });
        return response.data.hits;
    }
);

const searchSlice = createSlice({
    name: 'search',
    initialState: {
        query: '',
        results: [],
        status: 'idle',
        sourceFilter: 'all',
        error: null
    },
    reducers: {
        setQuery: (state, action) => {
            state.query = action.payload;
        },
        setSourceFilter: (state, action) => {
            state.sourceFilter = action.payload;
        },
        clearResults: (state) => {
            state.results = [];
            state.status = 'idle';
        }
    },
    extraReducers: (builder) => {
        builder
            .addCase(fetchSearchResults.pending, (state) => {
                state.status = 'loading';
            })
            .addCase(fetchSearchResults.fulfilled, (state, action) => {
                state.status = 'succeeded';
                state.results = action.payload;
            })
            .addCase(fetchSearchResults.rejected, (state, action) => {
                state.status = 'failed';
                state.error = action.error.message;
            });
    }
});

export const { setQuery, setSourceFilter, clearResults } = searchSlice.actions;
export default searchSlice.reducer;