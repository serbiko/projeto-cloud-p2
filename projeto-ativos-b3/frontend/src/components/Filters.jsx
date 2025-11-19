import React, { useState, useEffect } from 'react';
import { Box, TextField, Button, Autocomplete } from '@mui/material';
import { fetchAvailableTickers } from '../services/api';

export default function Filters({ onFilter }) {
  const [tickers, setTickers] = useState([]);
  const [ticker, setTicker] = useState('');
  const [dataInicio, setDataInicio] = useState('');
  const [dataFim, setDataFim] = useState('');

  useEffect(() => {
    loadTickers();
  }, []);

  const loadTickers = async () => {
    try {
      const data = await fetchAvailableTickers();
      setTickers(data);
    } catch (error) {
      console.error('Erro ao carregar tickers:', error);
    }
  };

  const handleSearch = () => {
    onFilter({
      ticker,
      dataInicio,
      dataFim,
      page: 0
    });
  };

  const handleClear = () => {
    setTicker('');
    setDataInicio('');
    setDataFim('');
    onFilter({});
  };

  return (
    <Box sx={{ mb: 3, p: 2, bgcolor: 'background.paper', borderRadius: 1, boxShadow: 1 }}>
      <Box display="flex" gap={2} flexWrap="wrap" alignItems="center">
        <Autocomplete
          value={ticker}
          onChange={(e, newValue) => setTicker(newValue || '')}
          options={tickers}
          sx={{ minWidth: 200 }}
          renderInput={(params) => (
            <TextField {...params} label="Ticker" placeholder="Ex: PETR4" size="small" />
          )}
        />
        
        <TextField
          label="Data Início"
          type="date"
          value={dataInicio}
          onChange={(e) => setDataInicio(e.target.value)}
          InputLabelProps={{ shrink: true }}
          sx={{ minWidth: 150 }}
          size="small"
        />
        
        <TextField
          label="Data Fim"
          type="date"
          value={dataFim}
          onChange={(e) => setDataFim(e.target.value)}
          InputLabelProps={{ shrink: true }}
          sx={{ minWidth: 150 }}
          size="small"
        />
        
        <Button variant="contained" onClick={handleSearch}>
          Buscar
        </Button>
        
        <Button variant="outlined" onClick={handleClear}>
          Limpar
        </Button>
      </Box>
    </Box>
  );
}