import React from 'react';
import {
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  TablePagination,
  Typography
} from '@mui/material';

export default function AssetTable({ data, page, rowsPerPage, onPageChange, onRowsPerPageChange }) {
  const formatCurrency = (value) => {
    return new Intl.NumberFormat('pt-BR', {
      style: 'currency',
      currency: 'BRL'
    }).format(value);
  };

  const formatNumber = (value) => {
    return new Intl.NumberFormat('pt-BR').format(value);
  };

  const formatDate = (dateString) => {
    return new Date(dateString + 'T00:00:00').toLocaleDateString('pt-BR');
  };

  if (!data?.content || data.content.length === 0) {
    return (
      <Paper sx={{ p: 3, textAlign: 'center' }}>
        <Typography variant="body1" color="text.secondary">
          Nenhum resultado encontrado
        </Typography>
      </Paper>
    );
  }

  return (
    <Paper sx={{ boxShadow: 2 }}>
      <TableContainer>
        <Table>
          <TableHead>
            <TableRow sx={{ bgcolor: 'primary.main' }}>
              <TableCell sx={{ color: 'white', fontWeight: 'bold' }}>Ticker</TableCell>
              <TableCell sx={{ color: 'white', fontWeight: 'bold' }}>Data</TableCell>
              <TableCell align="right" sx={{ color: 'white', fontWeight: 'bold' }}>Abertura</TableCell>
              <TableCell align="right" sx={{ color: 'white', fontWeight: 'bold' }}>Mínimo</TableCell>
              <TableCell align="right" sx={{ color: 'white', fontWeight: 'bold' }}>Máximo</TableCell>
              <TableCell align="right" sx={{ color: 'white', fontWeight: 'bold' }}>Fechamento</TableCell>
              <TableCell align="right" sx={{ color: 'white', fontWeight: 'bold' }}>Volume</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {data.content.map((row) => (
              <TableRow key={row.id} hover sx={{ '&:hover': { bgcolor: 'action.hover' } }}>
                <TableCell><strong>{row.ticker}</strong></TableCell>
                <TableCell>{formatDate(row.data_pregao)}</TableCell>
                <TableCell align="right">{formatCurrency(row.preco_abertura)}</TableCell>
                <TableCell align="right">{formatCurrency(row.preco_min)}</TableCell>
                <TableCell align="right">{formatCurrency(row.preco_max)}</TableCell>
                <TableCell align="right"><strong>{formatCurrency(row.preco_ultimo)}</strong></TableCell>
                <TableCell align="right">{formatNumber(row.quantidade_negociada)}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
      
      <TablePagination
        component="div"
        count={data.total}
        page={page}
        onPageChange={onPageChange}
        rowsPerPage={rowsPerPage}
        onRowsPerPageChange={onRowsPerPageChange}
        labelRowsPerPage="Registros por página:"
        labelDisplayedRows={({ from, to, count }) => `${from}-${to} de ${count}`}
      />
    </Paper>
  );
}