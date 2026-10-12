import axios from 'axios';
import { Toast } from '@douyinfe/semi-ui';
import type { Account, AccountDetailData, UserCardItem, TaskItem, StoreItem, StoreAppointment, NameMonitorPayload, JobLog, UserInfo, Order, OrderStats, DashboardChartData, BatchDailyResult, SystemSettings, NotifyTestResult, LocationSearchResult, ClawBotStatus, ClawBotQrResponse, ClawBotPollResponse, ClawBotActivationResponse, AccountNotifyConfig, AccountNotifyResponse, XiaoCanMessagesResponse } from '../types';
import {
  getStoreList, verifyToken,
  type RpcContext,
} from '../utils/xiaocan-direct';

export const API_BASE = import.meta.env.VITE_API_BASE || '/api';

// ============ 前端直连层 —— 走用户自己的 IP，不经过后端 ============
// 从 localStorage / 全局 store 拿到当前选中的账号 token、city_code 等上下文
function buildDirectCtx(key?: string): RpcContext {
  // 优先用传入的 key 查账号
  const accountsRaw = localStorage.getItem('xiaocan_accounts');
  const accounts: Record<string, any> = accountsRaw ? JSON.parse(accountsRaw) : {};
  let acc = key ? accounts[key] : null;
  if (!acc) {
    // 没传 key 就用第一个
    const firstKey = Object.keys(accounts)[0];
    acc = firstKey ? accounts[firstKey] : null;
  }
  const token: string = acc?.token || acc?.x_sivir || acc?.sivir || '';
  const cityCode: number = Number(acc?.city_code) || 410100;
  const silkId: number | undefined = acc?.silk_id != null ? Number(acc.silk_id) : undefined;
  const userId: number | undefined = acc?.user_id != null ? Number(acc.user_id) : undefined;
  return { token, cityCode, silkId, userId, platform: (acc?.platform === 'mini' ? 'mini' : 'app') };
}

async function buildStoreListDirect(params: Record<string, any>, _signal?: AbortSignal) {
  const accountKey = params.account_key;
  const ctx = buildDirectCtx(accountKey);
  const resp = await getStoreList(ctx, {
    longitude: params.longitude,
    latitude: params.latitude,
    city_code: params.city_code || ctx.cityCode,
    page_pv_id: params.page_pv_id || Math.random().toString(36).slice(2),
    ...Object.fromEntries(Object.entries(params).filter(([k]) => !['account_key', 'limit', 'offset', 'keyword', 'platform', 'condition', 'rebate_type', 'sort_by'].includes(k))),
  });
  const pagePvId = params.page_pv_id || resp.data?.data?.page_pv_id || Math.random().toString(36).slice(2);
  return {
    ok: resp.ok,
    stores: extractStoreList(resp.data),
    total: extractTotal(resp.data),
    source: 'direct-browser',
    from_direct: true,
    has_more: false,
    next_offset: 0,
    page_pv_id: pagePvId,
    error: resp.ok ? undefined : `直连失败: ${resp.error || resp.rawStatus?.msg}`,
    // 让类型兼容原来的 endpoint 返回
    raw_total: extractTotal(resp.data),
  };
}
function extractStoreList(data: any): any[] {
  if (!data?.data) return [];
  const d = data.data;
  return (d.promotion_list || d.feed_list || d.store_list || d.items || []) as any[];
}
function extractTotal(data: any): number {
  const d = data?.data || {};
  return Number(d.total) || Number(d.count) || 0;
}

const client = axios.create({
  baseURL: API_BASE,
  timeout: 30000,
});

// 全局响应拦截器：统一网络异常与安全兜底
client.interceptors.response.use(
  (response) => response,
  (error) => {
    if (axios.isCancel(error)) {
      return Promise.reject(error);
    }
    if (error.code === 'ECONNABORTED' || error.message?.includes('timeout')) {
      Toast.error('请求超时，请检查后端服务是否正常响应');
    } else if (error.message === 'Network Error') {
      Toast.error('网络连接失败，请确认后端服务已在 8690 端口启动');
    }
    return Promise.reject(error);
  }
);

// 在途 GET 请求复用池：避免同一时间点多个并发相同 GET 请求重复打到后端
const inFlightGetRequests = new Map<string, Promise<any>>();

const getQueryKey = (url: string, params?: any): string => {
  if (!params) return url;
  try {
    const searchParams = new URLSearchParams();
    const sortedKeys = Object.keys(params).sort();
    for (const key of sortedKeys) {
      const val = params[key];
      if (val !== undefined && val !== null) {
        searchParams.append(key, String(val));
      }
    }
    const qs = searchParams.toString();
    return qs ? `${url}?${qs}` : url;
  } catch {
    return `${url}?${JSON.stringify(params)}`;
  }
};

const originalGet = client.get.bind(client);

(client as any).get = function (url: string, config?: any): Promise<any> {
  // 如果调用方显式提供了 AbortSignal，不进行在途共享，保证 AbortSignal 精确控制
  if (config?.signal) {
    return originalGet(url, config);
  }
  const key = getQueryKey(url, config?.params);
  const existing = inFlightGetRequests.get(key);
  if (existing) {
    return existing;
  }
  const promise = originalGet(url, config)
    .finally(() => {
      inFlightGetRequests.delete(key);
    });
  inFlightGetRequests.set(key, promise);
  return promise;
};

export const api = {
  // 账号管理
  getAccounts: () => client.get<{ ok: boolean; accounts: Account[]; total: number }>('/accounts').then(r => r.data),
  createAccount: (data: Partial<Account>) => client.post<{ ok: boolean; account: Account }>('/accounts', data).then(r => r.data),
  updateAccount: (key: string, data: Partial<Account>) => client.post<{ ok: boolean; account: Account; message: string }>(`/accounts/${key}/profile`, data).then(r => r.data),
  syncAccount: (key: string) => client.post<{ ok: boolean; account: Account; message: string }>(`/accounts/${key}/sync`).then(r => r.data),
  getAccountDetail: (key: string) => client.get<AccountDetailData>(`/accounts/${key}/detail`).then(r => r.data),
  getAccountCards: (key: string, status = 0) => client.get<{ ok: boolean; cards: UserCardItem[]; status: number }>(`/accounts/${key}/cards`, { params: { status } }).then(r => r.data),
  deleteAccount: (key: string) => client.delete<{ ok: boolean; message: string }>(`/accounts/${key}`).then(r => r.data),
  getAccountNotifyConfig: (key: string) => client.get<AccountNotifyResponse>(`/accounts/${key}/notify-config`).then(r => r.data),
  saveAccountNotifyConfig: (key: string, notify_mode: string, notify_config: AccountNotifyConfig) => client.post<{ ok: boolean; message: string }>(`/accounts/${key}/notify-config`, { notify_mode, notify_config }).then(r => r.data),
  testAccountNotify: (key: string, notify_mode: string, notify_config: AccountNotifyConfig) => client.post<{ ok: boolean; message: string; channel?: string }>(`/accounts/${key}/test-notify`, { notify_mode, notify_config }).then(r => r.data),
  
  // 账号解析与嗅探接入
  parseToken: (rawText: string) => client.post<{ ok: boolean; token?: string; silk_id?: string; is_valid: boolean; message: string; exp_date?: string; city_code?: number; nickname?: string; client_mode?: string }>('/accounts/parse-token', { raw_text: rawText }).then(r => r.data),
  verifyToken: async (token: string, cityCode?: number) => {
    // 优先前端直连验证
    try {
      const result = await verifyToken({ token: token, cityCode: cityCode || 410100 });
      if (result.ok) {
        return { ok: true, valid: true, message: '前端直连验证通过（用户自己的 IP）' };
      }
    } catch { /* 继续走后端 */ }
    return client.post<{ ok: boolean; valid: boolean; message: string }>('/accounts/verify-token', { token, city_code: cityCode }).then(r => r.data);
  },
  startProxyCapture: (port?: number) => client.post<{ ok: boolean; message: string; status: any }>('/accounts/proxy-capture/start', { port }).then(r => r.data),
  stopProxyCapture: () => client.post<{ ok: boolean; message: string; status: any }>('/accounts/proxy-capture/stop').then(r => r.data),
  getProxyCaptureStatus: () => client.get<{ ok: boolean; status: any }>('/accounts/proxy-capture/status').then(r => r.data),

  // 电脑微信无感直连 (免配置·首选)
  scanWechatAccount: () => client.post<{ ok: boolean; account?: Account; message: string }>('/accounts/wechat-scan').then(r => r.data),
  startWechatListener: (timeout?: number) => client.post<{ ok: boolean; message: string; status: any }>('/accounts/wechat-listener/start', { timeout }).then(r => r.data),
  stopWechatListener: () => client.post<{ ok: boolean; message: string; status: any }>('/accounts/wechat-listener/stop').then(r => r.data),
  getWechatListenerStatus: () => client.get<{ ok: boolean; status: any }>('/accounts/wechat-listener/status').then(r => r.data),

  // 历史兼容
  startWinCapture: (port?: number, timeout?: number) => client.post<{ ok: boolean; message: string; status: any }>('/accounts/win_capture/start', { port, timeout }).then(r => r.data),
  stopWinCapture: () => client.post<{ ok: boolean; message: string; status: any }>('/accounts/win_capture/stop').then(r => r.data),
  getWinCaptureStatus: () => client.get<{ ok: boolean; status: any }>('/accounts/win_capture/status').then(r => r.data),
  installCaCert: () => client.post<{ ok: boolean; message: string; ca_installed?: boolean }>('/accounts/win_capture/install_ca').then(r => r.data),


  // 任务自动化
  getTasks: (accountKey: string) => client.get<{ ok: boolean; tasks: TaskItem[] }>(`/tasks?account_key=${accountKey}`).then(r => r.data),
  toggleTask: (data: { account_key: string; task_id: string; enabled: boolean; cron_time?: string; params?: Record<string, any> }) => client.post<{ ok: boolean; message: string }>('/tasks/toggle', data).then(r => r.data),
  runTaskNow: (accountKey: string, taskId: string) => client.post<{ ok: boolean; job_id?: string; output: string }>('/tasks/run', { account_key: accountKey, task_id: taskId }, { timeout: 60000 }).then(r => r.data),
  batchRunDaily: (accountKey: string) => client.post<BatchDailyResult>('/tasks/batch-run-daily', { account_key: accountKey }, { timeout: 60000 }).then(r => r.data),

  // 店铺与附近霸王餐 (优先前端直连，失败 fallback 后端)
  getStores: async (params?: { city_code?: number; longitude?: string; latitude?: string; account_key?: string; keyword?: string; platform?: string; condition?: string; rebate_type?: string; sort_by?: string; limit?: number; offset?: number; page_pv_id?: string }, signal?: AbortSignal) => {
    try {
      const direct = await buildStoreListDirect(params || {}, signal);
      if (direct.ok) return direct;
    } catch { /* fallback 后端 */ }
    return client.get<{ ok: boolean; stores: StoreItem[]; total: number; raw_total?: number; source?: string; has_more?: boolean; next_offset?: number; page_pv_id?: string; keyword?: string; error?: string }>('/store/list', { params, signal }).then(r => r.data);
  },
  scanDualRebateStores: (params?: { city_code?: number; longitude?: string; latitude?: string; account_key?: string; platform?: string; keyword?: string; max_stores?: number }) =>
    client.get<{
      ok: boolean;
      total_scanned_promotions: number;
      total_scanned_stores: number;
      dual_rebate_count: number;
      returned_count?: number;
      max_stores?: number;
      stores: StoreItem[];
      message?: string;
    }>('/store/dual-rebate-scan', { params, timeout: 120000 }).then(r => r.data),
  grabStoreNow: (data: { account_key: string; store_id?: string; store_name?: string; promotion_id: string; platform?: string; order_money?: number; rebate_price?: number; rebate_desc?: string; advance?: boolean; redpack_id?: string; redpack_name?: string; redpack_mode?: number }) =>
    client.post<{ ok: boolean; order_id?: number; can_monitor?: boolean; message: string }>('/store/grab-now', data).then(r => r.data),
  createAppointment: (data: Partial<StoreAppointment>) => client.post<{ ok: boolean; appointment_id: number; message: string }>('/store/appoint', data).then(r => r.data),
  createNameMonitor: (data: NameMonitorPayload) =>
    client.post<{ ok: boolean; appointment_id: number; message: string }>('/store/name-monitor', data).then(r => r.data),
  getAppointments: (accountKey?: string) => client.get<{ ok: boolean; appointments: StoreAppointment[]; total: number }>('/store/appointments', { params: { account_key: accountKey } }).then(r => r.data),
  stopAppointment: (id: number) => client.post<{ ok: boolean; message: string }>(`/store/appointments/${id}/stop`).then(r => r.data),
  cancelAppointment: (id: number) => client.delete<{ ok: boolean; message: string }>(`/store/appointments/${id}`).then(r => r.data),

  // 设备定位与逆地理编码
  searchLocation: (keyword: string) =>
    client.get<LocationSearchResult>('/location/search', { params: { keyword } }).then(r => r.data),
  resolveLocation: (latitude: number, longitude: number) =>
    client.get<{ ok: boolean; city_code: number; city_name: string; locality?: string; province?: string; address_name: string; latitude: string; longitude: string; source?: string; accuracy?: string }>('/location/resolve', { params: { latitude, longitude } }).then(r => r.data),
  getIpLocation: () =>
    client.get<{ ok: boolean; city_code: number; city_name: string; address_name: string; latitude: string; longitude: string; is_ip?: boolean }>('/location/ip').then(r => r.data),

  // 阿里云 NTP 高精度授时校准
  getTime: (force?: boolean) =>
    client.get<{ ok: boolean; timestamp: number; beijing_time: string; server: string; synced: boolean; offset: number }>('/time', { params: { force } }).then(r => r.data),

  // 霸王餐订单管理
  getOrders: (params?: { account_key?: string; status?: string; platform?: string; keyword?: string; limit?: number; offset?: number }) => 
    client.get<{ ok: boolean; orders: Order[]; total: number }>('/orders', { params }).then(r => r.data),
  getOrderStats: (accountKey?: string) => 
    client.get<{ ok: boolean; stats: OrderStats }>(`/orders/stats`, { params: { account_key: accountKey } }).then(r => r.data),
  getDashboardChartData: (accountKey?: string) =>
    client.get<{ ok: boolean; data: DashboardChartData }>('/dashboard/chart-data', { params: { account_key: accountKey } }).then(r => r.data),
  getMessages: (params?: { account_key?: string; channel_id?: number; page?: number; page_size?: number }) =>
    client.get<XiaoCanMessagesResponse>('/messages', { params }).then(r => r.data),
  markAllMessagesRead: (account_key: string) =>
    client.post<{ ok: boolean; message: string }>('/messages/read-all', { account_key }).then(r => r.data),
  createOrder: (data: Partial<Order>) => 
    client.post<{ ok: boolean; order: Order; message: string }>('/orders', data).then(r => r.data),
  updateOrder: (id: number, data: Partial<Order>) => 
    client.put<{ ok: boolean; order: Order; message: string }>(`/orders/${id}`, data).then(r => r.data),
  submitPlatformOrderId: (id: number, platformOrderId: string, receiptImg?: string) => 
    client.post<{ ok: boolean; order: Order; message: string }>(`/orders/${id}/submit-platform-id`, { platform_order_id: platformOrderId, receipt_img: receiptImg }).then(r => r.data),
  delayOrderVoucher: (id: number) =>
    client.post<{ ok: boolean; is_delayed: boolean; delay_time: number; message: string }>(`/orders/${id}/delay-voucher`).then(r => r.data),
  useOrderFreeCard: (id: number, data?: { platform_order_id?: string; user_free_card_id?: number }) =>
    client.post<{ ok: boolean; message: string }>(`/orders/${id}/use-free-card`, data || {}).then(r => r.data),
  deleteOrder: (id: number) => 
    client.delete<{ ok: boolean; message: string }>(`/orders/${id}`).then(r => r.data),

  // 日志与监控
  getJobs: (accountKey?: string, limit: number = 50) => client.get<{ ok: boolean; jobs: JobLog[] }>('/jobs', { params: { account_key: accountKey, limit } }).then(r => r.data),
  clearJobs: (accountKey?: string) => client.delete<{ ok: boolean; message: string }>('/jobs', { params: { account_key: accountKey } }).then(r => r.data),
  
  // 用户中心 (免付费状态)
  getMe: () => client.get<{ ok: boolean; user: UserInfo }>('/me').then(r => r.data),

  // 系统配置与通知管理
  getSettings: () => client.get<SystemSettings>('/settings').then(r => r.data),
  saveSettings: (data: Partial<SystemSettings>) => client.post<{ ok: boolean; message: string }>('/settings', data).then(r => r.data),
  testNotify: (channel: string, config: Record<string, any>) => 
    client.post<NotifyTestResult>('/settings/test-notify', { channel, config }).then(r => r.data),
  testTianditu: (data: { tianditu_key: string }) =>
    client.post<{ ok: boolean; message: string; data?: any }>('/settings/test-tianditu', data).then(r => r.data),
  getClawBotStatus: (customPath?: string) => 
    client.get<ClawBotStatus & { ok: boolean; message: string }>('/settings/clawbot/status', { params: { custom_path: customPath } }).then(r => r.data),
  generateClawBotQr: (localToken?: string) =>
    client.post<ClawBotQrResponse>('/settings/clawbot/qr', { local_token: localToken }).then(r => r.data),
  pollClawBotStatus: (qrcode: string, verifyCode?: string) =>
    client.get<ClawBotPollResponse>('/settings/clawbot/poll', { params: { qrcode, verify_code: verifyCode } }).then(r => r.data),
  checkClawBotActivation: () =>
    client.post<ClawBotActivationResponse>('/settings/clawbot/check-activation').then(r => r.data),
  unbindClawBot: () =>
    client.post<{ ok: boolean; message: string }>('/settings/clawbot/unbind').then(r => r.data),

  // 阿里云 NTP 高精度授时服务
  getBeijingTime: () =>
    client
      .get<{
        ok: boolean;
        timestamp: number;
        beijing_time: string;
        server: string;
        synced: boolean;
        offset: number;
      }>('/time')
      .then(r => r.data),
};
