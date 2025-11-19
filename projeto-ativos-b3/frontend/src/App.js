import React, { useState, useEffect } from 'react';
import {
  Container,
  AppBar,
  Toolbar,
  Typography,
  Box,
  CircularProgress,
  Alert
} from '@mui/material';
import Filters from './components/Filters';
import AssetTable from './components/AssetTable';
import AssetChart from './components/AssetChart';
import { fetchAssets, checkHealth } from './services/api';

export default function App() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [filters, setFilters] = useState({});
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(30);
  const [apiStatus, setApiStatus] = useState(true);

  useEffect(() => {
    checkHealth()
      .then(status => setApiStatus(status))
      .catch(() => setApiStatus(false));
  }, []);

  useEffect(() => {
    loadData();
    // eslint-disable-next-line
  }, [filters, page, rowsPerPage]);

  const loadData = async () => {
    setLoading(true);
    setError(null);
    
    try {
      const result = await fetchAssets({
        ...filters,
        page,
        size: rowsPerPage
      });
      setData(result);
    } catch (error) {
      console.error('Erro ao carregar dados:', error);
      setError(error.message);
    } finally {
      setLoading(false);
    }
  };

  const handleFilter = (newFilters) => {
    setFilters(newFilters);
    setPage(0);
  };

  const handleChangePage = (event, newPage) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  return (
    <>
      <AppBar position="static" sx={{ bgcolor: '#1976d2' }}>
        <Toolbar>
          <Typography variant="h6" sx={{ flexGrow: 1 }}>
            📊 Dashboard de Ativos B3
          </Typography>
          {!apiStatus && (
            <Typography variant="body2" sx={{ bgcolor: 'warning.main', px: 2, py: 0.5, borderRadius: 1 }}>
              ⚠️ API offline
            </Typography>
          )}
        </Toolbar>
      </AppBar>
      
      <Container maxWidth="xl" sx={{ mt: 3, mb: 3 }}>
        {error && (
          <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
            {error}
          </Alert>
        )}
        
        <Filters onFilter={handleFilter} />
        
        {loading ? (
          <Box display="flex" justifyContent="center" alignItems="center" p={5}>
            <CircularProgress size={60} />
          </Box>
        ) : (
          <Box display="grid" gap={3}>
            {data && data.content && data.content.length > 0 && (
              <AssetChart data={data} />
            )}
            <AssetTable
              data={data}
              page={page}
              rowsPerPage={rowsPerPage}
              onPageChange={handleChangePage}
              onRowsPerPageChange={handleChangeRowsPerPage}
            />
          </Box>
        )}
      </Container>
    </>
  );
}