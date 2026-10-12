/**
 * 小蚕霸王餐 —— 前端直连层
 * 用浏览器自己的出口 IP 直接 fetch gw.xiaocantech.com/rpc
 *
 * 签名算法从 Python signer.py 直翻：
 *   X-Garen = Date.now() (毫秒时间戳)
 *   X-Nami  = crypto.randomUUID() 或 fallback UUID
 *   X-Ashe  = md5( md5("server.method".toLowerCase()) + timestamp + nami )
 *
 * 浏览器直连天然规避代理池/WAF问题——每个用户用自己分散的 IP 请求
 */

// ============ 纯 JS MD5（无外部依赖）============
function safeAdd(a: number, b: number) {
  const lsw = (a & 0xffff) + (b & 0xffff);
  const msw = (a >> 16) + (b >> 16) + (lsw >> 16);
  return (msw << 16) | (lsw & 0xffff);
}
function rotl(n: number, c: number) {
  return (n << c) | (n >>> (32 - c));
}
function cmn(q: number, a: number, b: number, x: number, s: number, t: number) {
  return safeAdd(rotl(safeAdd(safeAdd(a, q), safeAdd(x, t)), s), b);
}
function ff(a: number, b: number, c: number, d: number, x: number, s: number, t: number) {
  return cmn((b & c) | ((~b) & d), a, b, x, s, t);
}
function gg(a: number, b: number, c: number, d: number, x: number, s: number, t: number) {
  return cmn((b & d) | (c & (~d)), a, b, x, s, t);
}
function hh(a: number, b: number, c: number, d: number, x: number, s: number, t: number) {
  return cmn(b ^ c ^ d, a, b, x, s, t);
}
function ii(a: number, b: number, c: number, d: number, x: number, s: number, t: number) {
  return cmn(c ^ (b | (~d)), a, b, x, s, t);
}
function toHex(n: number) {
  let s = '';
  for (let j = 0; j <= 3; j++) {
    let v = (n >> (j * 8 + 4)) & 0x0f; s += v.toString(16);
    v = (n >> (j * 8)) & 0x0f; s += v.toString(16);
  }
  return s;
}
function utf8(s: string) {
  return unescape(encodeURIComponent(s));
}
function md5Hex(str: string): string {
  const input = utf8(str);
  const nblk = ((input.length + 8) >> 6) + 1;
  const blks: number[] = new Array(nblk * 16).fill(0);
  for (let i = 0; i < blks.length; i++) blks[i] = 0;
  for (let i = 0; i < input.length; i++) blks[i >> 2] |= (input.charCodeAt(i) & 0xff) << ((i % 4) * 8);
  blks[input.length >> 2] |= 0x80 << ((input.length % 4) * 8);
  blks[nblk * 16 - 2] = input.length * 8;

  let a = 0x67452301, b = 0xefcdab89, c = 0x98badcfe, d = 0x10325476;
  for (let i = 0; i < blks.length; i += 16) {
    const oa = a, ob = b, oc = c, od = d;
    a = ff(a,b,c,d,blks[i+0],7,-680876936); d = ff(d,a,b,c,blks[i+1],12,-389564586);
    c = ff(c,d,a,b,blks[i+2],17, 606105819); b = ff(b,c,d,a,blks[i+3],22,-1044525330);
    a = ff(a,b,c,d,blks[i+4],7,-176418897); d = ff(d,a,b,c,blks[i+5],12, 1200080426);
    c = ff(c,d,a,b,blks[i+6],17,-1473231341); b = ff(b,c,d,a,blks[i+7],22,  -45705983);
    a = ff(a,b,c,d,blks[i+8],7, 1770035416); d = ff(d,a,b,c,blks[i+9],12,-1958414417);
    c = ff(c,d,a,b,blks[i+10],17,     -42063); b = ff(b,c,d,a,blks[i+11],22,-1990404162);
    a = ff(a,b,c,d,blks[i+12],7, 1804603682); d = ff(d,a,b,c,blks[i+13],12,  -40341101);
    c = ff(c,d,a,b,blks[i+14],17,-1502002290); b = ff(b,c,d,a,blks[i+15],22, 1236535329);
    a = gg(a,b,c,d,blks[i+1],5,-165796510); d = gg(d,a,b,c,blks[i+6],9,-1069501632);
    c = gg(c,d,a,b,blks[i+11],14,  643717713); b = gg(b,c,d,a,blks[i+0],20, -373897302);
    a = gg(a,b,c,d,blks[i+5],5, -701558691); d = gg(d,a,b,c,blks[i+10],9,   38016083);
    c = gg(c,d,a,b,blks[i+15],14, -660478335); b = gg(b,c,d,a,blks[i+4],20, -405537848);
    a = gg(a,b,c,d,blks[i+9],5,  568446438); d = gg(d,a,b,c,blks[i+14],9,-1019803690);
    c = gg(c,d,a,b,blks[i+3],14, -187363961); b = gg(b,c,d,a,blks[i+8],20, 1163531501);
    a = gg(a,b,c,d,blks[i+13],5,-1444681467); d = gg(d,a,b,c,blks[i+2],9,  -51403784);
    c = gg(c,d,a,b,blks[i+7],14, 1735328473); b = gg(b,c,d,a,blks[i+12],20,-1926607734);
    a = hh(a,b,c,d,blks[i+5],4,   -378558); d = hh(d,a,b,c,blks[i+8],11,-2022574463);
    c = hh(c,d,a,b,blks[i+11],16, 1839030562); b = hh(b,c,d,a,blks[i+14],23, -35309556);
    a = hh(a,b,c,d,blks[i+1],4,-1530992060); d = hh(d,a,b,c,blks[i+4],11, 1272893353);
    c = hh(c,d,a,b,blks[i+7],16, -155497632); b = hh(b,c,d,a,blks[i+10],23,-1094730640);
    a = hh(a,b,c,d,blks[i+13],4,  681279174); d = hh(d,a,b,c,blks[i+0],11, -35853722);
    c = hh(c,d,a,b,blks[i+3],16, -722521979); b = hh(b,c,d,a,blks[i+6],23,   76029189);
    a = hh(a,b,c,d,blks[i+9],4, -640364487); d = hh(d,a,b,c,blks[i+12],11, -421815835);
    c = hh(c,d,a,b,blks[i+15],16,  530742520); b = hh(b,c,d,a,blks[i+2],23, -995338651);
    a = ii(a,b,c,d,blks[i+0],6, -198630844); d = ii(d,a,b,c,blks[i+7],10, 1126891415);
    c = ii(c,d,a,b,blks[i+14],15,-1416354905); b = ii(b,c,d,a,blks[i+5],21, -57434055);
    a = ii(a,b,c,d,blks[i+12],6, 1700485571); d = ii(d,a,b,c,blks[i+3],10,-1894986606);
    c = ii(c,d,a,b,blks[i+10],15,   -1051523); b = ii(b,c,d,a,blks[i+1],21,-2054922799);
    a = ii(a,b,c,d,blks[i+8],6, 1873313359); d = ii(d,a,b,c,blks[i+15],10, -30611744);
    c = ii(c,d,a,b,blks[i+6],15,-1560198380); b = ii(b,c,d,a,blks[i+13],21, 1309151649);
    a = ii(a,b,c,d,blks[i+4],6, -145523070); d = ii(d,a,b,c,blks[i+11],10,-1120210379);
    c = ii(c,d,a,b,blks[i+2],15,  718787259); b = ii(b,c,d,a,blks[i+9],21, -343485551);
    a = ii(a,b,c,d,blks[i+14],6, -798955658); d = ii(d,a,b,c,blks[i+5],10, -343485551);
    c = ii(c,d,a,b,blks[i+12],15,  755456052); b = ii(b,c,d,a,blks[i+1],21,-1895863544);
    a = safeAdd(a, oa); b = safeAdd(b, ob); c = safeAdd(c, oc); d = safeAdd(d, od);
  }
  return toHex(a) + toHex(b) + toHex(c) + toHex(d);
}

// 自检
console.assert(md5Hex('hello') === '5d41402abc4b2a76b9719d911017c592', 'MD5 self-check failed');

// ============ UUID ============
function genUUID(): string {
  if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) {
    return crypto.randomUUID();
  }
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

// ============ 签名 ============
export interface RpcContext {
  token: string;
  cityCode: number;
  silkId?: string | number;
  userId?: string | number;
  platform?: 'app' | 'mini';
}

function generateSignHeaders(serverName: string, methodName: string, ctx: RpcContext): Record<string, string> {
  const timeMillis = Date.now();
  const nami = genUUID();
  const x = md5Hex(`${serverName}.${methodName}`.toLowerCase());
  const ashe = md5Hex(x + timeMillis + nami);
  const isMini = (ctx.platform || 'app') === 'mini';

  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'x-City': String(ctx.cityCode),
    'X-Garen': String(timeMillis),
    'X-Nami': nami,
    'x-Annie': 'XC',
    'servername': serverName,
    'methodname': methodName,
    'X-Ashe': ashe,
  };

  if (isMini) {
    headers['X-Platform'] = 'mini';
    headers['X-App-Sr'] = '20';
    headers['X-Version'] = '3.20.6.67';
  } else {
    headers['X-Platform'] = 'Android';
    headers['X-App-Sr'] = '10';
    headers['X-Version'] = '3.21.1';
  }

  if (ctx.silkId !== undefined && ctx.silkId !== null) headers['x-Teemo'] = String(ctx.silkId);
  if (ctx.userId !== undefined && ctx.userId !== null) headers['x-Vayne'] = String(ctx.userId);
  if (ctx.token) {
    const clean = ctx.token.replace('Bearer ', '').trim();
    headers['Authorization'] = `Bearer ${clean}`;
    headers['x-Sivir'] = clean;
  }
  return headers;
}

// ============ 直连 RPC ============
const GW_URL = 'https://gw.xiaocantech.com/rpc';
// const DIRECT_TIMEOUT = 25000; // fetch 没有原生 timeout，依赖 AbortSignal

export interface RpcResult<T = any> {
  ok: boolean;
  statusCode: number;
  data?: T;
  rawStatus?: { code: number; msg: string };
  error?: string;
  elapsed: number;
  fromDirect: true;
}

/**
 * 前端直连小蚕 RPC
 * @param serverName server 名，例如 Silkworm
 * @param methodName method 名，例如 SilkwormService.GetClientUserInfo
 * @param payload 请求体 JSON
 * @param ctx 账号上下文（token / city_code / silk_id）
 * @param retryTimes 失败重试次数（默认 2 —— 浏览器端出口干净率高，不用后端那么多）
 */
export async function invokeRpcDirect<T = any>(
  serverName: string,
  methodName: string,
  payload: Record<string, any>,
  ctx: RpcContext,
  retryTimes = 2,
  signal?: AbortSignal,
): Promise<RpcResult<T>> {
  let lastErr: string | undefined;
  for (let attempt = 0; attempt <= retryTimes; attempt++) {
    const headers = generateSignHeaders(serverName, methodName, ctx);
    const t0 = Date.now();
    try {
      const resp = await fetch(GW_URL, {
        method: 'POST',
        headers,
        body: JSON.stringify(payload),
        signal,
      });
      const elapsed = Date.now() - t0;

      if (resp.status === 403) {
        lastErr = `WAF 403`;
        // WAF 拦截 —— 如果 CORS 头缺失，浏览器会抛 CORS 错而不是拿到 403
        continue;
      }
      if (resp.status !== 200) {
        lastErr = `HTTP ${resp.status}`;
        continue;
      }

      let json: any;
      try { json = await resp.json(); } catch { lastErr = 'JSON 解析失败'; continue; }

      const biz = json.status;
      if (biz && biz.code !== 0) {
        // 业务错误——不重试
        return { ok: false, statusCode: 200, rawStatus: biz, data: json, elapsed, fromDirect: true };
      }

      return { ok: true, statusCode: 200, data: json, elapsed, fromDirect: true };
    } catch (e: any) {
      lastErr = e?.name || e?.message || '网络错误';
      // 网络/CORS/超时 —— 重试
    }
  }
  // 直连彻底失败 —— throw 让外层 catch 触发 fallback 后端代理池
  throw new Error(`前端直连小蚕失败: ${lastErr}`);
}

// ============ 业务快捷方法 ============

export function getUserInfo(ctx: RpcContext) {
  return invokeRpcDirect(
    'Silkworm', 'SilkwormService.GetClientUserInfo',
    { silk_id: ctx.silkId ?? 0, page_pv_id: Math.random().toString(36).slice(2), app_id: 10 },
    ctx,
  );
}

export function getCardList(ctx: RpcContext, status = 0, offset = 0, number = 100) {
  return invokeRpcDirect(
    'SilkwormCard', 'CardService.GetCardList',
    { silk_id: ctx.silkId ?? 0, status, offset, number, app_id: 10 },
    ctx,
  );
}

export function getCardNumber(ctx: RpcContext) {
  return invokeRpcDirect(
    'SilkwormCard', 'CardService.GetUserCardNumber',
    { silk_id: ctx.silkId ?? 0, app_id: 10 },
    ctx,
  );
}

export function getRedpackNum(ctx: RpcContext) {
  return invokeRpcDirect(
    'SilkwormLottery', 'SilkwormLotteryMobile.ListUserRedPack',
    { silk_id: ctx.silkId ?? 0, app_id: 10 },
    ctx,
  );
}

export function getRedpackList(ctx: RpcContext, page = 1, pageSize = 50) {
  return invokeRpcDirect(
    'SilkwormLottery', 'SilkwormLotteryMobile.ListUserRedPack',
    { silk_id: ctx.silkId ?? 0, page, page_size: pageSize, app_id: 10 },
    ctx,
  );
}

export function getUserTaskV2(ctx: RpcContext) {
  return invokeRpcDirect(
    'Silkworm', 'SilkwormService.GetUserTaskV2',
    { silk_id: ctx.silkId ?? 0, app_id: 10 },
    ctx,
  );
}

export function getStoreList(ctx: RpcContext, payload: Record<string, any>) {
  return invokeRpcDirect(
    'SilkwormFusion', 'FusionService.GetFeedPromotions',
    { silk_id: ctx.silkId ?? 0, app_id: 10, ...payload },
    ctx,
  );
}

export function verifyToken(ctx: RpcContext) {
  return getUserInfo(ctx); // 能拿到用户信息 = token 有效
}
