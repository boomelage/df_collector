import os
import pandas as pd
from model_settings import ms
from joblib import Parallel, delayed

class df_collector:
	def __init__(self,root=None):
		self.root = None

	def collect_df(self,f):
		return pd.read_csv(f)

	def collect_dfs(self, datadir, n_jobs=-1):
		files = [f for f in os.listdir(datadir) if f.endswith('.csv')]
		files = [os.path.join(datadir, f) for f in files]
		dfs = Parallel(n_jobs=n_jobs)(delayed(self.collect_df)(f) for f in files)
		return pd.concat(dfs,ignore_index=True)

	def cboe_spx_barriers(self):
		if self.root != None:
			datadir = os.path.join(self.root,ms.cboe_spx_barriers['dump'])
			return self.collect_dfs(datadir)
		else:
			print('define the instance root!')

	def cboe_spx_short_term_asians(self):
		if self.root != None:
			datadir = os.path.join(self.root,ms.cboe_spx_short_term_asians['dump'])
			return self.collect_dfs(datadir)
		else:
			print('define the instance root!')