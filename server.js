const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto');
const PORT=process.env.PORT||3000;
const DIR=process.env.DATA_DIR||path.join(__dirname,'data');
fs.mkdirSync(path.join(DIR,'files'),{recursive:true});
const DB=path.join(DIR,'db.json');
let db={users:{},msgs:[],sessions:{}};
try{db=JSON.parse(fs.readFileSync(DB,'utf8'))}catch(e){}
let t=null;function save(){clearTimeout(t);t=setTimeout(()=>fs.writeFile(DB,JSON.stringify(db),()=>{}),300)}
const streams={};
const id=()=>crypto.randomBytes(8).toString('hex');
function hash(p,s){return crypto.scryptSync(p,s,32).toString('hex')}
function body(req,max=9e6){return new Promise((ok,no)=>{let n=0,c=[];req.on('data',d=>{n+=d.length;if(n>max){no(new Error('big'));req.destroy()}else c.push(d)});req.on('end',()=>{try{ok(JSON.parse(Buffer.concat(c).toString()||'{}'))}catch(e){no(e)}})})}
function send(res,code,o){res.writeHead(code,{'Content-Type':'application/json'});res.end(JSON.stringify(o))}
function who(req,u){const t=req.headers['x-token']||u.searchParams.get('t');return db.sessions[t]||null}
function push(user,ev){(streams[user]||[]).forEach(r=>r.write('data: '+JSON.stringify(ev)+'\n\n'))}
const pub=m=>({id:m.id,from:m.from,to:m.to,text:m.text,file:m.file,ts:m.ts});
http.createServer(async(req,res)=>{
const u=new URL(req.url,'http://x');const p=u.pathname;
try{
if(req.method==='GET'&&(p==='/'||p==='/index.html')){res.writeHead(200,{'Content-Type':'text/html; charset=utf-8'});return res.end(fs.readFileSync(path.join(__dirname,'index.html')))}
if(p==='/api/register'&&req.method==='POST'){const b=await body(req,1e5);const n=String(b.name||'').trim().toLowerCase();if(!/^[a-z0-9_]{3,20}$/.test(n))return send(res,400,{error:'الاسم 3 إلى 20 حرف إنجليزي أو أرقام'});if(String(b.password||'').length<6)return send(res,400,{error:'الباسورد 6 حروف على الأقل'});if(db.users[n])return send(res,400,{error:'الاسم مأخود'});const s=id();db.users[n]={salt:s,h:hash(b.password,s)};const tk=id()+id();db.sessions[tk]=n;save();return send(res,200,{token:tk,name:n})}
if(p==='/api/login'&&req.method==='POST'){const b=await body(req,1e5);const n=String(b.name||'').trim().toLowerCase();const us=db.users[n];if(!us||hash(String(b.password||''),us.salt)!==us.h)return send(res,400,{error:'الاسم أو الباسورد غلط'});const tk=id()+id();db.sessions[tk]=n;save();return send(res,200,{token:tk,name:n})}
const me=who(req,u);
if(!p.startsWith('/api/'))return send(res,404,{error:'no'});
if(!me)return send(res,401,{error:'login'});
if(p==='/api/users'){const q=(u.searchParams.get('q')||'').toLowerCase();return send(res,200,Object.keys(db.users).filter(x=>x!==me&&x.includes(q)).slice(0,20))}
if(p==='/api/chats'){const m={};for(const x of db.msgs){if(x.from!==me&&x.to!==me)continue;const o=x.from===me?x.to:x.from;m[o]=x}return send(res,200,Object.entries(m).map(([k,v])=>({with:k,last:pub(v)})).sort((a,b)=>b.last.ts-a.last.ts))}
if(p==='/api/messages'){const w=u.searchParams.get('with');return send(res,200,db.msgs.filter(x=>(x.from===me&&x.to===w)||(x.from===w&&x.to===me)).slice(-200).map(pub))}
if(p==='/api/send'&&req.method==='POST'){const b=await body(req);const to=String(b.to||'');if(!db.users[to])return send(res,400,{error:'مفيش مستخدم بالاسم ده'});const m={id:id(),from:me,to,text:String(b.text||'').slice(0,4000),file:null,ts:Date.now()};
if(b.file&&b.file.data){const buf=Buffer.from(b.file.data,'base64');if(buf.length>6e6)return send(res,400,{error:'الملف كبير (الحد 6 ميجا)'});const fid=id();fs.writeFileSync(path.join(DIR,'files',fid),buf);m.file={id:fid,name:String(b.file.name||'file').slice(0,100),type:String(b.file.type||'application/octet-stream').slice(0,100),size:buf.length}}
if(!m.text&&!m.file)return send(res,400,{error:'فاضية'});db.msgs.push(m);save();push(to,{m:pub(m)});push(me,{m:pub(m)});return send(res,200,pub(m))}
if(p==='/api/stream'){res.writeHead(200,{'Content-Type':'text/event-stream','Cache-Control':'no-cache',Connection:'keep-alive'});res.write('retry: 1500\n\n');(streams[me]=streams[me]||[]).push(res);const hb=setInterval(()=>res.write(': hb\n\n'),20000);req.on('close',()=>{clearInterval(hb);streams[me]=streams[me].filter(x=>x!==res)});return}
if(p.startsWith('/api/file/')){const fid=p.split('/').pop().replace(/[^a-f0-9]/g,'');const m=db.msgs.find(x=>x.file&&x.file.id===fid&&(x.from===me||x.to===me));if(!m)return send(res,404,{error:'no'});res.writeHead(200,{'Content-Type':m.file.type,'Content-Disposition':'inline; filename*=UTF-8\'\''+encodeURIComponent(m.file.name),'Cache-Control':'private, max-age=86400'});return fs.createReadStream(path.join(DIR,'files',fid)).pipe(res)}
send(res,404,{error:'no'})
}catch(e){send(res,400,{error:'خطأ'})}
}).listen(PORT,()=>console.log('up '+PORT));
