import fs from 'fs';
import path from 'path';
import axios from 'axios';
import { parse } from 'csv-parse';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

type LeadRow = Record<string, string | undefined>;

function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function normalizePhone(raw?: string): string | null {
  if (!raw) return null;
  const digits = raw.replace(/\D+/g, '');
  if (!digits) return null;
  if (raw.trim().startsWith('+')) return `+${digits}`;
  if (digits.startsWith('55')) return `+${digits}`;
  return `+55${digits}`;
}

async function main() {
  const csvPath = process.env.CSV_PATH
    ? path.resolve(process.env.CSV_PATH)
    : path.resolve(__dirname, 'Planilhas', 'Vendas Kiwify Atualizado.csv');
  if (!fs.existsSync(csvPath)) {
    console.error('Arquivo CSV não encontrado em', csvPath);
    process.exit(1);
  }

  const urlBase = process.env.IMPORT_API_URL || 'http://localhost:3000';
  const listId = process.env.CLICKUP_DEFAULT_LIST_ID || process.env.LIST_ID || '';
  const endpoint = `${urlBase}/import${listId ? `?listId=${encodeURIComponent(listId)}` : ''}`;

  let processed = 0;
  let created = 0;
  let updated = 0;
  let skipped = 0;
  const errors: Array<{ line: number; email?: string; error: any }> = [];

  // Ler todas as linhas primeiro
  const records: LeadRow[] = [];
  const reader = fs
    .createReadStream(csvPath)
    .pipe(parse({ columns: true, trim: true, skip_empty_lines: true }));
  for await (const rec of reader as unknown as AsyncIterable<LeadRow>) {
    records.push(rec);
  }

  // Detectar esquema e coluna de status
  const headerOrder = records.length > 0 ? Object.keys(records[0] as Record<string, any>) : [];
  const isKiwify = headerOrder.includes('Produto');
  const statusCol = isKiwify ? 'Cadastrado?' : (headerOrder.includes('Cadastrado') ? 'Cadastrado' : 'Cadastrado');
  const statuses: string[] = records.map(r => ((r[statusCol] || '') as string).toString());
  function csvEscape(value: any): string {
    const s = value === null || value === undefined ? '' : String(value);
    // Sempre entre aspas e escape de aspas duplas
    return '"' + s.replace(/"/g, '""') + '"';
  }

  // Função para converter linha -> payload
  function toPayload(record: LeadRow) {
    if (isKiwify) {
      const produto = (record['Produto'] || '').trim();
      const nome = (record['Cliente'] || '').trim();
      const email = (record['Email'] || '').trim();
      const cpf = ((record['CPF'] || record['CPF / CNPJ']) || '').toString().trim();
      const telefone = (record['Celular'] || '').trim();
      const valor = ((record['Preço base do produto'] || '') as string).toString().trim();
      const data = (record['Data de Atualização'] || '').trim();
      return { produto, nome, email, cpf, telefone, valor, data };
    }
    // Esquema Green
    const produto = (record['Nome do produto'] || '').trim();
    const nome = (record['Nome do cliente'] || '').trim();
    const email = (record['Email do cliente'] || '').trim();
    const cpf = (record['CPF'] || '').toString().trim();
    const telefone = (record['Telefone'] || '').trim();
    const valor = (record['Valor da Venda'] || '').toString().trim();
    const data = (record['Data de pagamento'] || '').trim();
    return { produto, nome, email, cpf, telefone, valor, data };
  }

  // Parâmetros de execução
  const CONCURRENCY = Math.max(1, Number(process.env.CONCURRENCY || 4));
  const LIMIT_ROWS = Math.max(1, Number(process.env.LIMIT_ROWS || 100));
  const START_ROW = Math.max(2, Number(process.env.START_ROW || 2)); // 2 = primeira linha após header

  // Construir fila de alvos a partir do intervalo solicitado
  const targets: number[] = [];
  const startIdx = Math.max(0, START_ROW - 2);
  const endIdx = Math.min(records.length - 1, startIdx + LIMIT_ROWS - 1);
  for (let idx = startIdx; idx <= endIdx; idx++) {
    const row = records[idx];
    if (!row) continue;
    const mark = (((row as any)[statusCol] || '') as string).toString().trim().toLowerCase();
    if (mark !== 'sim') targets.push(idx);
  }

  // Escrita incremental no mesmo arquivo para evitar duplicidade em caso de interrupção
  function writeOutIncremental() {
    try {
      const lines: string[] = [];
      const hasStatusCol = headerOrder.includes(statusCol);
      const outHeader = hasStatusCol ? headerOrder : [...headerOrder, statusCol];
      lines.push(outHeader.map(csvEscape).join(','));
      for (let k = 0; k < records.length; k++) {
        const row = records[k] as Record<string, any>;
        const dataCols = outHeader.map(h => h === statusCol ? csvEscape(statuses[k] || row[h] || '') : csvEscape(row[h]));
        lines.push(dataCols.join(','));
      }
      fs.writeFileSync(csvPath, lines.join('\n'), 'utf8');
    } catch (e) {
      console.error('Falha ao gravar CSV (incremental):', e);
    }
  }

  const queue: Array<{ rec: LeadRow; idx: number }> = targets
    .map(idx => ({ rec: records[idx], idx }))
    .filter(item => !!item.rec) as Array<{ rec: LeadRow; idx: number }>;
  while (queue.length > 0) {
    const batch = queue.splice(0, CONCURRENCY);
    await Promise.all(
      batch.map(async ({ rec, idx }) => {
        const { produto, nome, email, cpf, telefone, valor, data } = toPayload(rec);
        const ln = idx + 2; // header=1
        if (!email || !nome || !produto) {
          skipped++;
          errors.push({ line: ln, email, error: 'Campos obrigatórios ausentes (email/nome/produto)' });
          statuses[idx] = 'erro';
          return;
        }
        const leadPayload = {
          leads: [
            {
              nome,
              email,
              whatsapp: normalizePhone(telefone),
              cpf,
              produtos: produto,
              valor,
              data,
            },
          ],
        };
        try {
          const res = await axios.post(endpoint, leadPayload, { headers: { 'Content-Type': 'application/json' }, timeout: 60000 });
          const result = res.data?.results?.[0];
          processed++;
          if (result?.action === 'created') created++;
          else if (result?.action === 'updated') updated++;
          else if (result?.action === 'skipped') {
            skipped++;
            errors.push({ line: ln, email, error: result?.error || 'skipped' });
          }
          statuses[idx] = (result?.action === 'created' || result?.action === 'updated') ? 'sim' : (result?.action === 'skipped' ? 'erro' : (statuses[idx] || ''));
        } catch (err: any) {
          skipped++;
          errors.push({ line: ln, email, error: err?.response?.data || err?.message });
          statuses[idx] = 'erro';
        }
      })
    );
    // gravar progresso a cada batch
    writeOutIncremental();
    await sleep(0);
  }

  // Gerar CSV final no mesmo arquivo com coluna "Cadastrado?"
  try {
    writeOutIncremental();
    console.log(`Atualizado: ${csvPath}`);
  } catch (e) {
    console.error('Falha ao gravar CSV de saída:', e);
  }

  console.log(
    JSON.stringify(
      { summary: { processed, created, updated, skipped }, errors },
      null,
      2
    )
  );
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});


