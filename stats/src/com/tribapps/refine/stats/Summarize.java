package com.tribapps.refine.stats;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONWriter;

import com.google.refine.commands.Command;
import com.google.refine.ProjectManager;
import com.google.refine.model.Project;
import com.google.refine.model.ColumnModel;
import com.google.refine.model.Column;
import com.google.refine.model.Row;
import com.google.refine.browsing.Engine;
import com.google.refine.browsing.FilteredRows;
import com.google.refine.browsing.RowVisitor;
import com.google.refine.util.ParsingUtilities;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math.stat.descriptive.rank.Median;

public class Summarize extends Command {
    protected RowVisitor createRowVisitor(Project project, int cellIndex, List<Float> values) throws Exception {
        return new RowVisitor() {
            int cellIndex;
            List<Float> values;
            
            public RowVisitor init(int cellIndex, List<Float> values) {
                this.cellIndex = cellIndex;
                this.values = values;
                return this;
            }
            
            @Override
            public void start(Project project) {
            	// nothing to do
            }
            
            @Override
            public void end(Project project) {
            	// nothing to do
            }
            
            public boolean visit(Project project, int rowIndex, Row row) {
                try {
                    Number val = (Number)row.getCellValue(this.cellIndex);
                    this.values.add(val.floatValue());
                } catch (Exception e) {
                }

                return false;
            }
        }.init(cellIndex, values);
    }


    public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        this.doGet(request, response);
    };
    
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {            
        try {
            ProjectManager.singleton.setBusy(true);
            Project project = getProject(request);
            ColumnModel columnModel = project.columnModel;
            Column column = columnModel.getColumnByName(request.getParameter("column_name"));
            int cellIndex = column.getCellIndex();

            List<Float> values = new ArrayList<Float>();

            Engine engine = new Engine(project);
            JSONObject engineConfig = null;

            try {
                engineConfig = ParsingUtilities.evaluateJsonStringToObject(request.getParameter("engine"));
            } catch (JSONException e) {
                // ignore
            }

            engine.initializeFromJSON(engineConfig);

            FilteredRows filteredRows = engine.getAllFilteredRows();
            filteredRows.accept(project, createRowVisitor(project, cellIndex, values));
            
            HashMap map = computeStatistics(values);
            JSONWriter writer = new JSONWriter(response.getWriter());

            writer.object();

            for (Iterator<Map.Entry> entries = map.entrySet().iterator(); entries.hasNext();) {
                Map.Entry entry = entries.next();
                writer.key(entry.getKey().toString());
                writer.value(entry.getValue().toString());
            }

            writer.endObject();
        } catch (Exception e) {
            respondException(response, e);
        } finally {
            ProjectManager.singleton.setBusy(false);
        }
    };

    public HashMap computeStatistics(List<Float> values) {
        HashMap map = new HashMap();
        HashMap<Float, Integer> modeMap = new HashMap<Float, Integer>();
        DescriptiveStatistics stats = new DescriptiveStatistics();

        for (Float f : values) {
            stats.addValue(f);
            
            Integer current = modeMap.get(f);
            if (current == null) {
                modeMap.put(f, new Integer(1));
            } else {
                modeMap.put(f, current + 1);
            }
        }

        Float mode = null;
        Integer high = -1;

        for (Iterator<Map.Entry<Float,Integer>> entries = modeMap.entrySet().iterator(); entries.hasNext();) {
            Map.Entry<Float,Integer> entry = entries.next();
            if (entry.getValue() > high) {
                mode = entry.getKey();
                high = entry.getValue();
            }
        }

        if (!(Double.isNaN(stats.getN()))) {
            map.put("count", stats.getN());
        }
        
        if (!(Double.isNaN(stats.getSum()))) {
            map.put("sum", stats.getSum());
        }

        if (!(Double.isNaN(stats.getMin()))) {
            map.put("min", stats.getMin());
        }

        if (!(Double.isNaN(stats.getMax()))) {
            map.put("max", stats.getMax());
        }

        if (!(Double.isNaN((stats.getMean())))) {
            map.put("mean", stats.getMean());
        }

        if (!(Double.isNaN((stats.apply(new Median()))))) {
            map.put("median", stats.apply(new Median()));
        }

        if (mode != null) {
            map.put("mode", mode);
        }

        if (!(Double.isNaN((stats.getStandardDeviation())))) {
            map.put("stddev", stats.getStandardDeviation());
        }
        
        if (!(Double.isNaN((stats.getVariance())))) {
            map.put("variance", stats.getVariance());
        }

        return map;
    }
}

