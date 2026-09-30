const STOCK_US_BODY = document.getElementById('stock-us-body');
const STOCK_IN_BODY = document.getElementById('stock-in-body');
const CRYPTO_BODY = document.getElementById('crypto-table-body');
const STATUS = document.getElementById('connection-status');

let stompClient = null;
const prices = {
    stock: {},
    crypto: {}
};

function switchTab(tab) {
    document.querySelectorAll('.tab-content').forEach(el => el.classList.add('hidden'));
    document.getElementById(`content-${tab}`).classList.remove('hidden');
    
    document.querySelectorAll('[id^="tab-"]').forEach(el => {
        el.classList.remove('tab-active');
        el.classList.add('text-slate-400');
    });
    document.getElementById(`tab-${tab}`).classList.add('tab-active');
    document.getElementById(`tab-${tab}`).classList.remove('text-slate-400');
}

async function addSymbol() {
    const symbol = document.getElementById('symbol-input').value.trim();
    const market = document.getElementById('market-input').value;
    const btn = document.getElementById('add-symbol-btn');

    if (!symbol) return alert('Please enter a symbol');

    btn.disabled = true;
    btn.innerText = 'Validating...';

    try {
        const response = await fetch(`/api/v1/symbols/add?symbol=${symbol}&market=${market}`, { method: 'POST' });
        const data = await response.json();

        if (response.ok) {
            alert(`Successfully added ${symbol}! It will start appearing once the next fetch cycle runs.`);
            document.getElementById('symbol-input').value = '';
        } else {
            alert(`Error: ${data.message || 'Could not add symbol'}`);
        }
    } catch (e) {
        alert('Server error while adding symbol');
    } finally {
        btn.disabled = false;
        btn.innerText = 'Add Asset';
    }
}

document.getElementById('add-symbol-btn').addEventListener('click', addSymbol);

function connect() {
    const socket = new SockJS('/ws');
    stompClient = Stomp.over(socket);
    
    stompClient.connect({}, frame => {
        STATUS.innerText = 'Connected';
        STATUS.classList.replace('bg-red-900', 'bg-green-900');
        STATUS.classList.replace('text-red-200', 'text-green-200');
        
        stompClient.subscribe('/topic/prices/all', message => {
            const update = JSON.parse(message.body);
            handlePriceUpdate(update);
        });
    }, error => {
        STATUS.innerText = 'Disconnected';
        STATUS.classList.replace('bg-green-900', 'bg-red-900');
        setTimeout(connect, 5000);
    });
}

function handlePriceUpdate(update) {
    const type = update.type.toLowerCase();
    const market = update.market;
    const symbol = update.symbol;
    const oldPrice = prices[type]?.[symbol]?.price || 0;
    
    if (!prices[type]) prices[type] = {};
    prices[type][symbol] = update;
    
    const rowId = `${type}-${market}-${symbol}`;
    let row = document.getElementById(rowId);
    
    let targetBody;
    if (type === 'stock') {
        targetBody = market === 'INDIA' ? STOCK_IN_BODY : STOCK_US_BODY;
    } else {
        targetBody = CRYPTO_BODY;
    }

    if (!row) {
        row = createRow(type, update);
        targetBody.appendChild(row);
        const waiting = targetBody.querySelector('.italic');
        if (waiting) waiting.remove();
    }

    updateRow(row, update, oldPrice);
}

function createRow(type, update) {
    const tr = document.createElement('tr');
    const market = update.market || 'unknown';
    tr.id = `${type}-${market}-${update.symbol}`;
    tr.className = 'border-b border-slate-700 transition-colors duration-500';
    tr.innerHTML = `
        <td class="p-4 font-medium">${update.symbol}</td>
        <td class="p-4 text-right font-mono price-val">-</td>
        <td class="p-4 text-right font-mono change-val">-</td>
        <td class="p-4 text-right font-mono meta-val">-</td>
    `;
    return tr;
}

function updateRow(row, update, oldPrice) {
    const priceEl = row.querySelector('.price-val');
    const changeEl = row.querySelector('.change-val');
    const metaEl = row.querySelector('.meta-val');
    const price = parseFloat(update.price).toFixed(2);
    const change = parseFloat(update.changePercent || update.change).toFixed(2);
    const meta = update.type === 'STOCK' ? 
        (update.volume ? parseInt(update.volume).toLocaleString() : '-') : 
        (update.volume ? parseFloat(update.volume).toLocaleString() : '-');

    priceEl.innerText = `$${price}`;
    changeEl.innerText = `${change}%`;
    metaEl.innerText = meta;
    changeEl.className = `p-4 text-right font-mono change-val ${change >= 0 ? 'price-up' : 'price-down'}`;
    if (oldPrice !== 0) {
        row.classList.remove('flash-green', 'flash-red');
        void row.offsetWidth; 
        row.classList.add(parseFloat(update.price) >= oldPrice ? 'flash-green' : 'flash-red');
    }
}

connect();