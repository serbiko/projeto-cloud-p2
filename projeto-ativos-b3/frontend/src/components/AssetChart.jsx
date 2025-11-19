import React from 'react';
import { Paper, Typography } from '@mui/material';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer
} from 'recharts';

export default function AssetChart({ data }) {
  if (!data?.content || data.content.length === 0) {
    return null;
  }

  const chartData = data.content
    .slice()
    .reverse()
    .map(item => ({
      data: new Date(item.data_pregao + 'T00:00:00').toLocaleDateString('pt-BR', {
        day: '2-digit',
        month: '2-digit'
      }),
      Abertura: item.preco_abertura,
      Fechamento: item.preco_ultimo,
      Mínimo: item.preco_min,
      Máximo: item.preco_max
    }));

  return (
    <Paper sx={{ p: 2, boxShadow: 2 }}>
      <Typography variant="h6" gutterBottom sx={{ color: 'primary.main' }}>
        📈 Evolução de Preços
      </Typography>
      
      <ResponsiveContainer width="100%" height={350}>
        <LineChart data={chartData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis 
            dataKey="data" 
            angle={-45}
            textAnchor="end"
            height={80}
          />
          <YAxis />
          <Tooltip 
            formatter={(value) => `R$ ${value.toFixed(2)}`}
          />
          <Legend />
          <Line 
            type="monotone" 
            dataKey="Abertura" 
            stroke="#8884d8" 
            name="Abertura"
            strokeWidth={2}
          />
          <Line 
            type="monotone" 
            dataKey="Fechamento" 
            stroke="#82ca9d" 
            name="Fechamento"
            strokeWidth={2}
          />
          <Line 
            type="monotone" 
            dataKey="Mínimo" 
            stroke="#ff7300" 
            name="Mínimo"
            strokeWidth={1}
            strokeDasharray="5 5"
          />
          <Line 
            type="monotone" 
            dataKey="Máximo" 
            stroke="#ff0000" 
            name="Máximo"
            strokeWidth={1}
            strokeDasharray="5 5"
          />
        </LineChart>
      </ResponsiveContainer>
    </Paper>
  );
}