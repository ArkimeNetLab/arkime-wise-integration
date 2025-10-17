const WISESource = require('./wiseSource.js');
const axios = require('axios');

class MyService extends WISESource {
  constructor(api, section) {
    super(api, section, { dontCache: true });

    this.host = api.getConfig('myService', 'host') || '127.0.0.1';

    // --- existing fields ---
    this.appProtocolField = this.api.addField(
      'field:myService.app_protocol;db:myService.app_protocol;kind:string;friendly:App Protocol'
    );
    this.appCategoryField = this.api.addField(
      'field:myService.app_category;db:myService.app_category;kind:string;friendly:App Category'
    );
    this.appRiskField = this.api.addField(
      'field:myService.app_risk;db:myService.app_risk;kind:string;friendly:App Risk'
    );

    // --- new fields (from enriched dataset) ---
    this.durationField = this.api.addField(
      'field:myService.duration;db:myService.duration;kind:float;friendly:Flow Duration (s)'
    );
    this.protocolField = this.api.addField(
      'field:myService.protocol;db:myService.protocol;kind:string;friendly:L4 Protocol'
    );
    this.src2dstBytesField = this.api.addField(
      'field:myService.src2dst_bytes;db:myService.src2dst_bytes;kind:integer;friendly:Src→Dst Bytes'
    );
    this.dst2srcBytesField = this.api.addField(
      'field:myService.dst2src_bytes;db:myService.dst2src_bytes;kind:integer;friendly:Dst→Src Bytes'
    );
    this.src2dstPktsField = this.api.addField(
      'field:myService.src2dst_packets;db:myService.src2dst_packets;kind:integer;friendly:Src→Dst Packets'
    );
    this.dst2srcPktsField = this.api.addField(
      'field:myService.dst2src_packets;db:myService.dst2src_packets;kind:integer;friendly:Dst→Src Packets'
    );
    this.dataRatioField = this.api.addField(
      'field:myService.data_ratio;db:myService.data_ratio;kind:float;friendly:Data Ratio'
    );
    this.iatFlowAvgField = this.api.addField(
      'field:myService.iat_flow_avg;db:myService.iat_flow_avg;kind:float;friendly:IAT Flow Avg'
    );
    this.pktlenC2SAvgField = this.api.addField(
      'field:myService.pktlen_c_to_s_avg;db:myService.pktlen_c_to_s_avg;kind:float;friendly:C→S Pktlen Avg'
    );
    this.pktlenS2CAvgField = this.api.addField(
      'field:myService.pktlen_s_to_c_avg;db:myService.pktlen_s_to_c_avg;kind:float;friendly:S→C Pktlen Avg'
    );
    this.tcpAckField = this.api.addField(
      'field:myService.tcp_ack_count;db:myService.tcp_ack_count;kind:integer;friendly:TCP ACK Count'
    );
    this.tcpPshField = this.api.addField(
      'field:myService.tcp_psh_count;db:myService.tcp_psh_count;kind:integer;friendly:TCP PSH Count'
    );
    this.encryptedField = this.api.addField(
      'field:myService.encrypted;db:myService.encrypted;kind:integer;friendly:Encrypted'
    );
    this.breedField = this.api.addField(
      'field:myService.breed;db:myService.breed;kind:string;friendly:App Breed'
    );
    this.confidenceField = this.api.addField(
      'field:myService.confidence;db:myService.confidence;kind:string;friendly:Detection Confidence'
    );
    this.riskScoreField = this.api.addField(
      'field:myService.risk_score_total;db:myService.risk_score_total;kind:integer;friendly:Total Risk Score'
    );

    // --- ordering machinery ---
    this._queue = [];
    this._seq = 0;
    this._workerRunning = false;

    this.api.addSource('myService', this, ['tuple']);
  }

  _parseTupleKey(tupleKey) {
    const raw = String(tupleKey).trim();
    const parts = raw.split(',');
    if (parts.length === 2) {
      const [left, right] = parts;
      const [idOrTime, l4] = left.split(';');
      const r = right.split(';').filter(Boolean);
      if (r.length < 5) return null;
      let [proto, sip, sport, dip, dport] = r.slice(-5);
      if (/^\d+$/.test(proto)) proto = proto === '6' ? 'TCP' : (proto === '17' ? 'UDP' : proto);
      else proto = (proto || '').toUpperCase();
      const tsec = /^\d+$/.test(idOrTime) ? parseInt(idOrTime, 10) : undefined;
      return { sip, sport, dip, dport, proto, tsec };
    }
    const toks = raw.split(/[;|\s]+/).filter(Boolean);
    if (toks.length >= 5) {
      let [proto, sip, sport, dip, dport] = toks.slice(-5);
      if (/^\d+$/.test(proto)) proto = proto === '6' ? 'TCP' : (proto === '17' ? 'UDP' : proto);
      else proto = (proto || '').toUpperCase();
      return { sip, sport, dip, dport, proto, tsec: undefined };
    }
    return null;
  }

  _enqueue(task) {
    this._queue.push(task);
    this._queue.sort((a, b) => (a.tsec - b.tsec) || (a.seq - b.seq));
    if (!this._workerRunning) this._runWorker();
  }

  async _runWorker() {
    this._workerRunning = true;
    while (this._queue.length) {
      const task = this._queue.shift();
      try {
        await task.run();
      } catch (e) {
        try { task.cb(null, this.emptyResult); } catch {}
      }
    }
    this._workerRunning = false;
  }

  getTuple(tupleKey, cb) {
    const tag = '[myService/getTuple]';
    try {
      const t = this._parseTupleKey(tupleKey);
      if (!t) {
        console.warn(tag, 'parse failed');
        return cb(null, this.emptyResult);
      }

      if (!this.host) {
        console.error(tag, 'NO HOST configured (myService.host).');
        return cb(null, this.emptyResult);
      }

      const url = `http://${this.host}:5000/enrich`;
      const params = {
        src_ip: t.sip,
        dest_ip: t.dip,
        src_port: t.sport,
        dst_port: t.dport,
        proto: t.proto,
        t: t.tsec
      };

      axios.get(url, { params, timeout: 5000 })
        .then(({ data }) => {
          let riskStr = 'test**Risk';
          if (Array.isArray(data?.app_risk)) {
            riskStr = data.app_risk
              .map(r => {
                const score = r.score ? JSON.stringify(r.score) : '';
                return `id=${r.id}, risk="${r.risk}", severity=${r.severity}, score=${score}`;
              })
              .join(' | ');
          } else if (typeof data?.app_risk === 'object') {
            riskStr = JSON.stringify(data.app_risk);
          } else if (data?.app_risk) {
            riskStr = String(data.app_risk);
          }

          // --- Encode all enriched fields for Arkime ---
	 console.log(`flask api response : ${data.dst2src_packets}`);
          const buf = WISESource.encodeResult(
            this.appCategoryField, data?.app_category ?? 'N/A',
            this.appProtocolField, data?.app_protocol ?? 'N/A',
            this.appRiskField, riskStr,
            this.durationField, data?.duration ?? 0,
            this.protocolField, data?.protocol ?? 'N/A',
            this.src2dstBytesField, data?.src2dst_bytes ?? 0,
            this.dst2srcBytesField, data?.dst2src_bytes ?? 0,
            this.src2dstPktsField, data?.src2dst_packets ?? 0,
            this.dst2srcPktsField, data?.dst2src_packets ?? 0,
            this.dataRatioField, data?.data_ratio ?? 0,
            this.iatFlowAvgField, data?.iat_flow_avg ?? 0,
            this.pktlenC2SAvgField, data?.pktlen_c_to_s_avg ?? 0,
            this.pktlenS2CAvgField, data?.pktlen_s_to_c_avg ?? 0,
            this.tcpAckField, data?.tcp_ack_count ?? 0,
            this.tcpPshField, data?.tcp_psh_count ?? 0,
            this.encryptedField, data?.encrypted ?? 0,
            this.breedField, data?.breed ?? 'N/A',
            this.confidenceField, data?.confidence ?? 'N/A',
            this.riskScoreField, data?.risk_score_total ?? 0
          );
//           console.log(buf); 
          cb(null, buf);
        })
        .catch((e) => {
          console.error(tag, 'ERROR:', e?.message || e);
          cb(null, this.emptyResult);
        });

    } catch (e) {
      console.error(tag, 'ERROR:', e?.message || e);
      cb(null, this.emptyResult);
    }
  }
}

exports.initSource = function (api) {
  api.addSourceConfigDef('myService', {
    singleton: true,
    name: 'myService',
    description: 'Ordered Flask enrichment',
    link: 'https://arkime.com/wisesources',
    types: ['tuple'],
    cacheable: false,
    displayable: true,
    fields: [{ name: 'host', required: true, help: 'Flask host (no scheme/port)' }]
  });
  return new MyService(api, 'myService');
};
