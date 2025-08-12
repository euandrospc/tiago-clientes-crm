import Fastify from 'fastify';
import axios from 'axios';
import { z } from 'zod';
import dotenv from 'dotenv';

dotenv.config();

const app = Fastify({ logger: true });

const QuerySchema = z.object({
  listId: z.string().min(1).optional()
});

const TaskInputSchema = z.object({
  name: z.string().min(1),
  description: z.string().optional(),
  status: z.string().optional(),
  priority: z.number().int().min(1).max(4).optional(),
  dueDate: z.string().optional(),
  startDate: z.string().optional(),
  assignees: z.array(z.string()).optional(),
  tags: z.array(z.string()).optional(),
  customFields: z.record(z.string(), z.any()).optional()
});

const LeadInputSchema = z.object({
  nome: z.string().min(1),
  email: z.string().email().optional(),
  whatsapp: z.string().optional(),
  cpf: z.string().optional(),
  produtos: z.union([z.string(), z.array(z.string())]).optional(),
  estado: z.string().min(2).max(2).optional(),
  endereco_back: z.string().optional(),
  description: z.string().optional(),
  priority: z.number().int().min(1).max(4).optional(),
  // Dados de compra (opcionais)
  valor: z.union([z.string(), z.number()]).optional(),
  data: z.string().optional(),
});

const BodySchema = z.union([
  z.object({ task: TaskInputSchema }),
  z.object({ lead: LeadInputSchema }),
]);

const ImportBodySchema = z.object({
  leads: z.array(LeadInputSchema).nonempty(),
});

type ListCustomField = {
  id: string;
  name: string;
  type: string;
  type_config?: any;
};

function normalize(str: string): string {
  return str
    .toLowerCase()
    .normalize('NFD')
    .replace(/\p{Diacritic}/gu, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function toArray(input?: string | string[]): string[] {
  if (!input) return [];
  if (Array.isArray(input)) return input.map(v => `${v}`.trim()).filter(Boolean);
  return input
    .split(/[,;|]/)
    .map(v => v.trim())
    .filter(Boolean);
}

function formatPhoneE164(raw?: string): string | null {
  if (!raw) return null;
  const digits = (raw || '').replace(/\D+/g, '');
  if (!digits) return null;
  if (raw.trim().startsWith('+')) {
    return `+${digits}`;
  }
  // Assume BR if no country code provided
  if (digits.startsWith('55')) return `+${digits}`;
  return `+55${digits}`;
}

function buildCurl(method: string, url: string, headers: Record<string, string>, body?: any): string {
  const safeHeaders = { ...headers } as Record<string, string>;
  if (safeHeaders.Authorization) {
    const token = safeHeaders.Authorization;
    safeHeaders.Authorization = token.length > 12
      ? token.slice(0, 6) + '***' + token.slice(-4)
      : '***MASKED***';
  }
  const headerParts = Object.entries(safeHeaders)
    .map(([k, v]) => `-H '${k}: ${v}'`)
    .join(' \\\n  ');
  const dataPart = body !== undefined ? ` \\\n  -d '${JSON.stringify(body).replace(/'/g, "'\''")}'` : '';
  return `curl -X ${method.toUpperCase()} '${url}' \\\n  ${headerParts}${dataPart}`;
}

function parseCurrencyToNumber(input?: string | number | null): number | null {
  if (input === null || input === undefined) return null;
  if (typeof input === 'number') return Number.isFinite(input) ? input : null;
  let s = `${input}`.trim();
  if (!s) return null;
  s = s.replace(/BRL|R\$|\s/gi, '');
  if (/[,]/.test(s) && /[.]/.test(s)) {
    s = s.replace(/\./g, '');
    s = s.replace(/,/g, '.');
  } else if (/,/.test(s)) {
    s = s.replace(/,/g, '.');
  }
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function toUSDMoneyString(value: number): string {
  return value.toFixed(2);
}

function toClickUpCurrency(value: number): string {
  return Number.isInteger(value) ? String(Math.trunc(value)) : value.toFixed(2);
}

function parseDateInput(dateInput?: string): Date {
  if (!dateInput) return new Date();
  const s = String(dateInput).trim();
  // Formato esperado: DD/MM/YYYY [HH:mm[:ss]]
  const m = s.match(/^([0-3]\d)\/([0-1]\d)\/(\d{4})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?$/);
  if (m) {
    const dd = Number(m[1]);
    const mm = Number(m[2]);
    const yyyy = Number(m[3]);
    const HH = m[4] !== undefined ? Number(m[4]) : 0;
    const MM = m[5] !== undefined ? Number(m[5]) : 0;
    const SS = m[6] !== undefined ? Number(m[6]) : 0;
    // new Date(year, monthIndex, day, hours, minutes, seconds)
    return new Date(yyyy, Math.max(0, mm - 1), dd, HH, MM, SS);
  }
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? new Date() : d;
}

function formatDateBR(dateInput?: string): string {
  const date = parseDateInput(dateInput);
  const dd = String(date.getDate()).padStart(2, '0');
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const yyyy = date.getFullYear();
  return `${dd}/${mm}/${yyyy}`;
}

function buildPurchaseLine(dateIso: string, amount: number, product: string): string {
  const dateBr = formatDateBR(dateIso);
  return `${dateBr} | ${toUSDMoneyString(amount)} | ${product}`;
}
// Mapa de DDD para UF (Brasil)
const DDD_TO_UF: Record<string, string> = {
  '68': 'AC',
  '82': 'AL',
  '96': 'AP',
  '92': 'AM', '97': 'AM',
  '71': 'BA', '73': 'BA', '74': 'BA', '75': 'BA', '77': 'BA',
  '85': 'CE', '88': 'CE',
  '61': 'DF',
  '27': 'ES', '28': 'ES',
  '62': 'GO', '64': 'GO',
  '98': 'MA', '99': 'MA',
  '65': 'MT', '66': 'MT',
  '67': 'MS',
  '31': 'MG', '32': 'MG', '33': 'MG', '34': 'MG', '35': 'MG', '37': 'MG', '38': 'MG',
  '91': 'PA', '93': 'PA', '94': 'PA',
  '83': 'PB',
  '41': 'PR', '42': 'PR', '43': 'PR', '44': 'PR', '45': 'PR', '46': 'PR',
  '81': 'PE', '87': 'PE',
  '86': 'PI', '89': 'PI',
  '21': 'RJ', '22': 'RJ', '24': 'RJ',
  '84': 'RN',
  '51': 'RS', '53': 'RS', '54': 'RS', '55': 'RS',
  '69': 'RO',
  '95': 'RR',
  '47': 'SC', '48': 'SC', '49': 'SC',
  '11': 'SP', '12': 'SP', '13': 'SP', '14': 'SP', '15': 'SP', '16': 'SP', '17': 'SP', '18': 'SP', '19': 'SP',
  '79': 'SE',
  '63': 'TO',
};

function extractDDDFromE164(phoneE164?: string | null): string | null {
  if (!phoneE164) return null;
  // Esperado: +55DDxxxxxxxxx
  if (!phoneE164.startsWith('+55')) return null;
  const rest = phoneE164.slice(3);
  const ddd = rest.slice(0, 2);
  return ddd && /\d{2}/.test(ddd) ? ddd : null;
}

async function getListCustomFields(listId: string): Promise<ListCustomField[]> {
  const res = await axios.get(`https://api.clickup.com/api/v2/list/${encodeURIComponent(listId)}/field`, {
    headers: { Authorization: process.env.CLICKUP_TOKEN || '' },
    timeout: 20000,
  });
  return res.data?.fields ?? res.data ?? [];
}

async function getTaskById(taskId: string): Promise<any> {
  const url = `https://api.clickup.com/api/v2/task/${taskId}`;
  const headers = { Authorization: process.env.CLICKUP_TOKEN || '' };
  const res = await axios.get(url, { headers, timeout: 20000 });
  return res.data;
}

async function setCustomFieldValue(taskId: string, fieldId: string, value: any): Promise<void> {
  const headers = {
    Authorization: process.env.CLICKUP_TOKEN || '',
    'Content-Type': 'application/json',
    accept: 'application/json',
  } as any;
  const body = { value };
  // Tentar primeiro sem /value (evita uma falha extra quando a variação sem sufixo é a correta)
  let url = `https://api.clickup.com/api/v2/task/${encodeURIComponent(taskId)}/field/${encodeURIComponent(fieldId)}`;
  console.log('[cURL] Set Custom Field (try1):', buildCurl('POST', url, headers, body));
  try {
    await axios.post(url, body, { headers, timeout: 20000 });
    return;
  } catch (e: any) {
    const status = e?.response?.status;
    if (status !== 404 && status !== 405) throw e;
  }
  // Tentativa 2: endpoint com /value (variação da API)
  url = `https://api.clickup.com/api/v2/task/${encodeURIComponent(taskId)}/field/${encodeURIComponent(fieldId)}/value`;
  console.log('[cURL] Set Custom Field (try2):', buildCurl('POST', url, headers, body));
  await axios.post(url, body, { headers, timeout: 20000 });
}

async function addTaskComment(taskId: string, commentText: string): Promise<void> {
  const url = `https://api.clickup.com/api/v2/task/${taskId}/comment`;
  const headers = {
    Authorization: process.env.CLICKUP_TOKEN || '',
    'Content-Type': 'application/json',
  };
  const body = { comment_text: commentText } as any;
  console.log('[cURL] Add Comment:', buildCurl('POST', url, headers, body));
  await axios.post(url, body, { headers, timeout: 15000 });
}

async function addTagToTask(taskId: string, tagName: string): Promise<void> {
  const safeTag = tagName.trim();
  if (!safeTag) return;
  const url = `https://api.clickup.com/api/v2/task/${taskId}/tag/${encodeURIComponent(safeTag)}`;
  const headers = { Authorization: process.env.CLICKUP_TOKEN || '', accept: 'application/json' } as any;
  console.log('[cURL] Add Tag:', buildCurl('POST', url, headers));
  await axios.post(url, undefined, { headers, timeout: 15000 });
}

// Fallbacks/overrides de IDs de custom fields conhecidos
const CPF_CUSTOM_FIELD_ID = process.env.CLICKUP_CPF_FIELD_ID || '82ed3673-9b3e-4c7f-8c21-b070b7ecd3e1';
const EMAIL_CUSTOM_FIELD_ID = process.env.CLICKUP_EMAIL_FIELD_ID || 'c34aaeb2-0233-42d3-8242-cd9a603b5b0b';
const VALOR_VENDA_CUSTOM_FIELD_ID = process.env.CLICKUP_VALOR_VENDA_FIELD_ID || '34343626-c252-4f75-acd3-d28391885d4b';

app.post('/tasks', async (req, reply) => {
  const { listId: listIdFromQuery } = QuerySchema.parse(req.query);
  const parsed = BodySchema.safeParse(req.body);

  if (!parsed.success) {
    return reply.code(400).send({ error: true, message: parsed.error.flatten() });
  }

  const listId = listIdFromQuery ?? process.env.CLICKUP_DEFAULT_LIST_ID;
  if (!listId) {
    return reply.code(400).send({ error: true, message: 'listId ausente (query) e CLICKUP_DEFAULT_LIST_ID não definido.' });
  }

  // Se for input no formato lead
  if ('lead' in parsed.data) {
    const lead = parsed.data.lead;
    const productList = toArray(lead.produtos);
    const tagsSet = new Set<string>();
    if (productList.length > 0) {
      for (const p of productList) tagsSet.add(normalize(p));
    }
    // Derivar UF pelo DDD do telefone (prioritário)
    let phoneE164 = formatPhoneE164(lead.whatsapp) || null;
    const ddd = extractDDDFromE164(phoneE164);
    const ufFromDDD = ddd ? DDD_TO_UF[ddd] : undefined;
    if (ufFromDDD) {
      tagsSet.add(normalize(ufFromDDD)); // vira "rj", "sp" ...
    } else if (lead.estado) {
      tagsSet.add(normalize(lead.estado));
    }
    const tags = Array.from(tagsSet);

    try {
      // Buscar custom fields e mapear por nome normalizado (antes da criação, para já enviar no payload)
      let fields: ListCustomField[] = [];
      try {
        fields = await getListCustomFields(listId);
      } catch (e) {
        req.log.warn({ e }, 'Falha ao buscar custom fields da lista');
      }
      const byName = new Map<string, ListCustomField>();
      for (const f of fields) byName.set(normalize(f.name), f);

      // Mapas de nomes esperados
      const emailField = byName.get(normalize('Email'));
      const whatsappField = byName.get(normalize('WhatsApp')) || byName.get(normalize('Whatsapp'));
      const cpfField = byName.get(normalize('CPF'));
      const produtosField = byName.get(normalize('Produto')) || byName.get(normalize('Produtos'));
      // Montar custom_fields para enviar já na criação
      const customFieldsForCreate: Array<{ id: string; value: any }> = [];
      if (lead.email && emailField) customFieldsForCreate.push({ id: emailField.id, value: lead.email });
      if (lead.whatsapp && whatsappField) {
        const phone = formatPhoneE164(lead.whatsapp);
        if (phone) customFieldsForCreate.push({ id: whatsappField.id, value: phone });
      }
      if (lead.cpf) {
        if (cpfField) customFieldsForCreate.push({ id: cpfField.id, value: lead.cpf });
        else if (CPF_CUSTOM_FIELD_ID) customFieldsForCreate.push({ id: CPF_CUSTOM_FIELD_ID, value: lead.cpf });
      }
      if (productList.length > 0 && produtosField) {
        // Mapear nomes das labels para IDs de opção
        const options = (produtosField.type_config?.options || []) as Array<{ id: string; label?: string; name?: string }>;
        const normalizedLabelToId = new Map<string, string>();
        const optionIdsSet = new Set<string>(options.map(o => `${o.id}`));
        for (const opt of options) {
          const labelText = (opt.label ?? opt.name ?? '').toString();
          if (labelText) normalizedLabelToId.set(normalize(labelText), `${opt.id}`);
        }

        // Tentar casar por ID ou por nome normalizado
        const chosenOptionIds: string[] = [];
        for (const raw of productList) {
          const trimmed = `${raw}`.trim();
          if (!trimmed) continue;
          // Se já for um ID válido presente nas opções, use direto
          if (optionIdsSet.has(trimmed)) {
            chosenOptionIds.push(trimmed);
            continue;
          }
          // Caso contrário, tente casar por nome
          const byNameId = normalizedLabelToId.get(normalize(trimmed));
          if (byNameId) {
            chosenOptionIds.push(byNameId);
          } else {
            // Log para depuração: opção não encontrada
            console.warn('[Produtos] opção não encontrada nas labels do campo', {
              solicitado: trimmed,
              disponiveis: options.map(o => ({ id: o.id, label: (o.label ?? o.name ?? '').toString() }))
            });
          }
        }

        if (chosenOptionIds.length > 0) {
          // labels espera array de IDs
          customFieldsForCreate.push({ id: produtosField.id, value: chosenOptionIds });
          console.log('[Produtos] IDs escolhidos para labels:', chosenOptionIds);
        } else {
          console.warn('[Produtos] nenhum ID de label correspondente encontrado; campo não será enviado.');
        }
      }

      // Descrição com registro de compra, se houver
      let purchaseAmountCreate = parseCurrencyToNumber((lead as any).valor);
      const purchaseDateCreate = (lead as any).data || new Date().toISOString();
      const firstProductCreate = productList[0];
      let descriptionCreate = lead.description;
      if (purchaseAmountCreate && firstProductCreate) {
        const line = buildPurchaseLine(purchaseDateCreate, purchaseAmountCreate, firstProductCreate);
        descriptionCreate = `${descriptionCreate ? descriptionCreate + '\n\n' : ''}Compras:\n- ${line}`;
      }

      const createPayload: any = {
        name: lead.nome,
        description: descriptionCreate,
        priority: lead.priority,
        tags,
      };
      // Valor da Venda (se informado na criação)
      if (purchaseAmountCreate && VALOR_VENDA_CUSTOM_FIELD_ID) {
        customFieldsForCreate.push({ id: VALOR_VENDA_CUSTOM_FIELD_ID, value: toClickUpCurrency(purchaseAmountCreate) });
      }
      if (customFieldsForCreate.length > 0) createPayload.custom_fields = customFieldsForCreate;

      const url = `https://api.clickup.com/api/v2/list/${encodeURIComponent(listId)}/task`;
      const headers = {
        Authorization: process.env.CLICKUP_TOKEN || '',
        'Content-Type': 'application/json',
      };
      // Log cURL
      console.log('[cURL] Create Task (lead):', buildCurl('POST', url, headers, createPayload));
      const res = await axios.post(url, createPayload, { headers, timeout: 30000 });

      const createdTask = res.data;

      return reply.code(201).send(createdTask);
    } catch (err: any) {
      const status = err?.response?.status || 500;
      const data = err?.response?.data;
      req.log.error({ err: data || err.message }, 'ClickUp error (lead)');
      return reply.code(status).send({ error: true, message: data || err.message });
    }
  }

  // Caso contrário, fluxo original por "task"
  const { task } = parsed.data as { task: z.infer<typeof TaskInputSchema> };
  const payload: any = {
    name: task.name,
    description: task.description,
    status: task.status,
    priority: task.priority,
    assignees: task.assignees,
    tags: task.tags,
  };

  if (task.dueDate) payload.due_date = new Date(task.dueDate).getTime();
  if (task.startDate) payload.start_date = new Date(task.startDate).getTime();

  try {
    const url = `https://api.clickup.com/api/v2/list/${encodeURIComponent(listId)}/task`;
    const headers = {
      Authorization: process.env.CLICKUP_TOKEN || '',
      'Content-Type': 'application/json',
    };
    // Log cURL
    console.log('[cURL] Create Task (task):', buildCurl('POST', url, headers, payload));
    const res = await axios.post(url, payload, { headers, timeout: 30000 });

    const createdTask = res.data;

    if (task.customFields && Object.keys(task.customFields).length > 0) {
      for (const [fieldId, value] of Object.entries(task.customFields)) {
        await setCustomFieldValue(createdTask.id, fieldId, value);
      }
    }

    return reply.code(201).send(createdTask);
  } catch (err: any) {
    const status = err?.response?.status || 500;
    const data = err?.response?.data;
    req.log.error({ err: data || err.message }, 'ClickUp error');
    return reply.code(status).send({ error: true, message: data || err.message });
  }
});

async function getTasksPage(listId: string, page: number): Promise<any[]> {
  const url = `https://api.clickup.com/api/v2/list/${encodeURIComponent(listId)}/task`;
  const headers = { Authorization: process.env.CLICKUP_TOKEN || '' };
  const params = { page, archived: false, include_subtasks: true, subtasks: true } as any;
  const res = await axios.get(url, { headers, params, timeout: 30000 });
  return res.data?.tasks ?? [];
}

async function findTaskByEmailOnList(listId: string, email: string, fieldIds?: string[]): Promise<any | null> {
  const target = (email || '').trim().toLowerCase();
  if (!target) return null;
  const idsToCheck = (fieldIds && fieldIds.length > 0) ? fieldIds.map(id => `${id}`) : [EMAIL_CUSTOM_FIELD_ID];
  for (let page = 0; page < 50; page++) {
    const tasks = await getTasksPage(listId, page);
    if (!tasks.length) break;
    for (const t of tasks) {
      const cfs = (t.custom_fields || []) as Array<{ id: string; value?: any }>;
      const cf = cfs.find(f => idsToCheck.includes(`${f.id}`) && typeof f.value === 'string');
      if (cf && `${(cf.value as string).trim().toLowerCase()}` === target) return t;
    }
  }
  return null;
}

// Busca otimizada usando filtro de custom_fields na própria API (ClickUp v2)
async function findTaskByEmailUsingFilter(listId: string, email: string, fieldId: string): Promise<any | null> {
  const value = (email || '').trim();
  if (!value) return null;
  const url = `https://api.clickup.com/api/v2/list/${encodeURIComponent(listId)}/task`;
  const headers = { Authorization: process.env.CLICKUP_TOKEN || '' };
  const filter = [{ field_id: fieldId, value }];
  const params: any = {
    include_subtasks: true,
    archived: false,
    custom_fields: JSON.stringify(filter),
  };
  try {
    const res = await axios.get(url, { headers, params, timeout: 30000 });
    const tasks = res.data?.tasks || [];
    return tasks[0] || null;
  } catch (err) {
    return null;
  }
}

function unionTags(existing: string[] = [], additions: string[] = []): string[] {
  const set = new Set<string>();
  for (const t of existing) set.add(normalize(t));
  for (const t of additions) set.add(normalize(t));
  return Array.from(set);
}

// Lock simples por e-mail para evitar corrida entre requisições
const processingEmails = new Set<string>();
function sleepMs(ms: number) { return new Promise(resolve => setTimeout(resolve, ms)); }
async function withEmailLock<T>(email: string | undefined, fn: () => Promise<T>): Promise<T> {
  const key = (email || '').trim().toLowerCase();
  if (!key) return fn();
  while (processingEmails.has(key)) {
    await sleepMs(25);
  }
  processingEmails.add(key);
  try { return await fn(); } finally { processingEmails.delete(key); }
}

app.post('/import', async (req, reply) => {
  const { listId: listIdFromQuery } = QuerySchema.parse(req.query);
  const parsed = ImportBodySchema.safeParse(req.body);
  if (!parsed.success) {
    return reply.code(400).send({ error: true, message: parsed.error.flatten() });
  }
  const listId = listIdFromQuery ?? process.env.CLICKUP_DEFAULT_LIST_ID;
  if (!listId) {
    return reply.code(400).send({ error: true, message: 'listId ausente (query) e CLICKUP_DEFAULT_LIST_ID não definido.' });
  }

  // Pre-carregar metadados da lista
  let fields: ListCustomField[] = [];
  try { fields = await getListCustomFields(listId); } catch {}
  const byName = new Map<string, ListCustomField>();
  for (const f of fields) byName.set(normalize(f.name), f);
  const emailField = byName.get(normalize('Email'));
  const whatsappField = byName.get(normalize('WhatsApp')) || byName.get(normalize('Whatsapp'));
  const cpfField = byName.get(normalize('CPF'));
  const produtosField = byName.get(normalize('Produto')) || byName.get(normalize('Produtos'));

  const results: Array<{ email?: string; action: 'created' | 'updated' | 'skipped'; taskId?: string; error?: any }>
    = [];

  for (const lead of parsed.data.leads) {
    try {
      await withEmailLock(lead.email, async () => {
      // Tags e produtos
      const productList = toArray(lead.produtos);
      const tagsSet = new Set<string>();
      for (const p of productList) tagsSet.add(normalize(p));
      const phone = formatPhoneE164(lead.whatsapp);
      const ddd = extractDDDFromE164(phone);
      const ufFromDDD = ddd ? DDD_TO_UF[ddd] : undefined;
      if (ufFromDDD) tagsSet.add(normalize(ufFromDDD));
      else if (lead.estado) tagsSet.add(normalize(lead.estado));
      const derivedTags = Array.from(tagsSet);

      // Mapear produtos -> IDs (com aproximação)
      let produtoOptionIds: string[] = [];
      if (productList.length > 0 && produtosField) {
        const options = (produtosField.type_config?.options || []) as Array<{ id: string; label?: string; name?: string }>;
        const normalizedLabelToId = new Map<string, string>();
        const optionIdsSet = new Set<string>(options.map(o => `${o.id}`));
        const optionList = options.map(opt => {
          const labelText = (opt.label ?? opt.name ?? '').toString();
          const norm = normalize(labelText);
          if (labelText) normalizedLabelToId.set(norm, `${opt.id}`);
          return { id: `${opt.id}`, label: labelText, norm };
        });
        for (const raw of productList) {
          const trimmed = `${raw}`.trim();
          if (!trimmed) continue;
          if (optionIdsSet.has(trimmed)) produtoOptionIds.push(trimmed);
          else {
            const norm = normalize(trimmed);
            const byNameId = normalizedLabelToId.get(norm);
            if (byNameId) produtoOptionIds.push(byNameId);
            else {
              // Matching aproximado: contém/é contido
              const candidate = optionList.find(o => o.norm.includes(norm) || norm.includes(o.norm));
              if (candidate) produtoOptionIds.push(candidate.id);
            }
          }
        }
        produtoOptionIds = Array.from(new Set(produtoOptionIds));
      }

      // Procurar por e-mail usando ID dinâmico do campo de e-mail
      let existing: any | null = null;
      if (lead.email) {
        const emailFieldId = (emailField?.id || EMAIL_CUSTOM_FIELD_ID);
        existing = await findTaskByEmailUsingFilter(listId, lead.email, emailFieldId);
        if (!existing) existing = await findTaskByEmailOnList(listId, lead.email, [emailFieldId, EMAIL_CUSTOM_FIELD_ID]);
      }
      if (existing) {
        // Atualizar
        // Buscar task atual para mesclar tags e produtos
        const taskId = existing.id;
        const currentTags: string[] = (existing.tags || []).map((t: any) => `${t.name}`);
        const tags = unionTags(currentTags, derivedTags);

        // Mesclar labels de produto
        let currentProdutoIds: string[] = [];
        let currentProdutoNames: string[] = [];
        if (produtosField) {
          // Buscar estado mais atual da task para garantir valores corretos
          let freshTask: any = existing;
          try { freshTask = await getTaskById(taskId); } catch {}
          const currentCf = (freshTask.custom_fields || []).find((f: any) => `${f.id}` === produtosField.id);
          if (currentCf && Array.isArray(currentCf.value)) currentProdutoIds = currentCf.value.map((v: any) => `${v}`);
          // nomes atuais para comentário
          const options = (produtosField.type_config?.options || []) as Array<{ id: string; label?: string; name?: string }>;
          const idToName = new Map(options.map(o => [`${o.id}`, (o.label ?? o.name ?? '').toString()]));
          currentProdutoNames = (currentProdutoIds || []).map(id => idToName.get(id) || id);
        }
        const mergedProdutoIds = Array.from(new Set([...(currentProdutoIds || []), ...(produtoOptionIds || [])]));
        // nomes finais para comentário
        let mergedProdutoNames: string[] = currentProdutoNames;
        if (produtosField) {
          const options = (produtosField.type_config?.options || []) as Array<{ id: string; label?: string; name?: string }>;
          const idToName = new Map(options.map(o => [`${o.id}`, (o.label ?? o.name ?? '').toString()]));
          mergedProdutoNames = (mergedProdutoIds || []).map(id => idToName.get(id) || id);
        }

        const updatePayload: any = {};
        if (lead.nome && lead.nome !== existing.name) updatePayload.name = lead.nome;
        // Por restrição da API, não editar mais de um custom field por requisição.
        // Portanto, não enviaremos custom_fields no PUT; atualizaremos cada campo via endpoint específico após o PUT.
        const fieldUpdates: Array<{ id: string; value: any }> = [];
        if (lead.email && (emailField || EMAIL_CUSTOM_FIELD_ID)) fieldUpdates.push({ id: (emailField?.id || EMAIL_CUSTOM_FIELD_ID), value: lead.email });
        if (phone && whatsappField) fieldUpdates.push({ id: whatsappField.id, value: phone });
        if (lead.cpf) fieldUpdates.push({ id: (cpfField?.id || CPF_CUSTOM_FIELD_ID), value: lead.cpf });
        if (mergedProdutoIds.length > 0 && produtosField) fieldUpdates.push({ id: produtosField.id, value: mergedProdutoIds });
        // Valor da venda (somatório)
        let purchaseAmountUpdate = parseCurrencyToNumber((lead as any).valor);
        let newTotalForComment: number | null = null;
        if (purchaseAmountUpdate && VALOR_VENDA_CUSTOM_FIELD_ID) {
          // Buscar valor mais atual do campo diretamente da task
          let currentVal = 0;
          try {
            const fresh = await getTaskById(taskId);
            const currentCf = (fresh.custom_fields || []).find((f: any) => `${f.id}` === VALOR_VENDA_CUSTOM_FIELD_ID);
            currentVal = parseCurrencyToNumber(currentCf?.value as any) || 0;
          } catch {}
          newTotalForComment = currentVal + purchaseAmountUpdate;
          // Não enviar via PUT custom_fields; usar endpoint específico de set field
        }
        // Não incluir custom_fields no PUT (iremos setar 1 a 1 após).

        // Atualizar descrição com a nova compra
        const firstProductUpdate = productList[0];
        if (purchaseAmountUpdate && firstProductUpdate) {
          const purchaseDateUpdate = (lead as any).data || new Date().toISOString();
          const line = buildPurchaseLine(purchaseDateUpdate, purchaseAmountUpdate, firstProductUpdate);
          // Buscar descrição mais atual
          let existingDesc: string = existing.description || existing.text_content || '';
          try {
            const fresh = await getTaskById(taskId);
            existingDesc = fresh.description || fresh.text_content || existingDesc;
          } catch {}
          let newDesc: string;
          if (/^\s*Compras:/m.test(existingDesc)) {
            newDesc = existingDesc.includes(line) ? existingDesc : `${existingDesc}\n- ${line}`;
          } else if (existingDesc.trim().length > 0) {
            newDesc = `${existingDesc}\n\nCompras:\n- ${line}`;
          } else {
            newDesc = `Compras:\n- ${line}`;
          }
          updatePayload.description = newDesc;
        }

        const url = `https://api.clickup.com/api/v2/task/${taskId}`;
        const headers = { Authorization: process.env.CLICKUP_TOKEN || '', 'Content-Type': 'application/json' };
        console.log('[cURL] Update Task (import):', buildCurl('PUT', url, headers, updatePayload));
        await axios.put(url, updatePayload, { headers, timeout: 30000 });
        // Atualizar custom fields um a um
        for (const f of fieldUpdates) {
          try { await setCustomFieldValue(taskId, f.id, f.value); } catch {}
        }
        // Atualizar campo de Valor da Venda usando endpoint específico
        if (purchaseAmountUpdate && VALOR_VENDA_CUSTOM_FIELD_ID && newTotalForComment !== null) {
          await setCustomFieldValue(taskId, VALOR_VENDA_CUSTOM_FIELD_ID, toClickUpCurrency(newTotalForComment));
        }
        // Adicionar apenas tags novas, uma a uma (não remove anteriores)
        try {
          const existingNorm = new Set(currentTags.map(t => normalize(t)));
          for (const tag of tags) {
            const norm = normalize(tag);
            if (!existingNorm.has(norm)) {
              await addTagToTask(taskId, tag);
              existingNorm.add(norm);
            }
          }
        } catch (e) {
          // segue sem falhar a importação inteira
        }
        // Comentário explicando a atualização
        const commentLines: string[] = [];
        commentLines.push(`Atualização automática de lead (${lead.email || lead.nome || ''}).`);
        if (currentProdutoNames || mergedProdutoNames) {
          commentLines.push(`Produtos: [antes] ${currentProdutoNames.join(', ') || '-'} → [depois] ${mergedProdutoNames.join(', ') || '-'}`);
        }
        commentLines.push(`Tags: [antes] ${currentTags.join(', ') || '-'} → [depois] ${tags.join(', ') || '-'}`);
        if (lead.nome && lead.nome !== existing.name) commentLines.push(`Nome: '${existing.name}' → '${lead.nome}'`);
        if (lead.cpf) commentLines.push(`CPF atualizado.`);
        if (phone) commentLines.push(`WhatsApp atualizado (${phone}).`);
        if (purchaseAmountUpdate && firstProductUpdate) {
          const commentPurchaseDate = (lead as any).data || new Date().toISOString();
          commentLines.push(`Compra registrada: ${formatDateBR(commentPurchaseDate)} | ${toUSDMoneyString(purchaseAmountUpdate)} | ${firstProductUpdate}.`);
        }
        if (newTotalForComment !== null) commentLines.push(`Valor da Venda (total): ${toClickUpCurrency(newTotalForComment)}.`);
        await addTaskComment(taskId, commentLines.join('\n'));
        results.push({ ...(lead.email ? { email: lead.email } : {}), action: 'updated', taskId });
      } else {
        // Criar
        const createPayload: any = { name: lead.nome, tags: derivedTags };
        const cfArray: Array<{ id: string; value: any }> = [];
        if (lead.email && (emailField || EMAIL_CUSTOM_FIELD_ID)) cfArray.push({ id: (emailField?.id || EMAIL_CUSTOM_FIELD_ID), value: lead.email });
        const phone = formatPhoneE164(lead.whatsapp);
        if (phone && whatsappField) cfArray.push({ id: whatsappField.id, value: phone });
        if (lead.cpf) cfArray.push({ id: (cpfField?.id || CPF_CUSTOM_FIELD_ID), value: lead.cpf });
        if (produtoOptionIds.length > 0 && produtosField) cfArray.push({ id: produtosField.id, value: produtoOptionIds });
        // Valor da venda na criação
        const purchaseAmountCreate = parseCurrencyToNumber((lead as any).valor);
        if (purchaseAmountCreate && VALOR_VENDA_CUSTOM_FIELD_ID) {
          cfArray.push({ id: VALOR_VENDA_CUSTOM_FIELD_ID, value: toClickUpCurrency(purchaseAmountCreate) });
        }
        if (cfArray.length > 0) createPayload.custom_fields = cfArray;

        // Descrição com compras
        const firstProductCreate = productList[0];
        const purchaseDateCreate = (lead as any).data || new Date().toISOString();
        if (purchaseAmountCreate && firstProductCreate) {
          const line = buildPurchaseLine(purchaseDateCreate, purchaseAmountCreate, firstProductCreate);
          createPayload.description = `Compras:\n- ${line}`;
        }

        const url = `https://api.clickup.com/api/v2/list/${encodeURIComponent(listId)}/task`;
        const headers = { Authorization: process.env.CLICKUP_TOKEN || '', 'Content-Type': 'application/json' };
        console.log('[cURL] Create Task (import):', buildCurl('POST', url, headers, createPayload));
        const cr = await axios.post(url, createPayload, { headers, timeout: 30000 });
        results.push({ ...(lead.email ? { email: lead.email } : {}), action: 'created', taskId: cr.data?.id });
      }
      }); // end withEmailLock
    } catch (err: any) {
      results.push({ ...(lead.email ? { email: lead.email } : {}), action: 'skipped', error: err?.response?.data || err?.message });
    }
  }

  return reply.send({ ok: true, results });
});

app.get('/health', async (req, reply) => {
  return reply.send({ status: 'ok', timestamp: new Date().toISOString() });
});

const port = Number(process.env.PORT || 3000);
const host = process.env.HOST || '0.0.0.0';

app.listen({ port, host })
  .then(() => app.log.info(`API running on http://${host}:${port}`))
  .catch(err => {
    app.log.error(err, 'Failed to start');
    process.exit(1);
  });


