import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

export const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

api.interceptors.response.use(
  (response) => response,
  (error) => {
    console.error('Erro na requisição:', error);
    
    if (error.response) {
      const message = error.response.data?.detail || 'Erro ao comunicar com o servidor';
      throw new Error(message);
    } else if (error.request) {
      throw new Error('Servidor não respondeu. Verifique sua conexão.');
    } else {
      throw new Error('Erro ao processar requisição');
    }
  }
);

export const fetchAssets = async (filters = {}) => {
  const params = {};
  
  if (filters.ticker) params.q = filters.ticker;
  if (filters.dataInicio) params.from = filters.dataInicio;
  if (filters.dataFim) params.to = filters.dataFim;
  if (filters.page !== undefined) params.page = filters.page;
  if (filters.size) params.size = filters.size;
  
  const response = await api.get('/api/assets', { params });
  return response.data;
};

export const fetchAssetById = async (id) => {
  const response = await api.get(`/api/assets/${id}`);
  return response.data;
};

export const fetchAvailableTickers = async () => {
  const response = await api.get('/api/assets/meta/tickers');
  return response.data.tickers || [];
};

export const fetchAvailableDates = async (limit = 30) => {
  const response = await api.get('/api/assets/meta/dates', {
    params: { limit }
  });
  return response.data.dates || [];
};

export const checkHealth = async () => {
  try {
    const response = await api.get('/api/health');
    return response.data.status === 'ok';
  } catch (error) {
    return false;
  }
};