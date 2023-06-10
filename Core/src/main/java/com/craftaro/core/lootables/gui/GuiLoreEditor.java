package com.craftaro.core.lootables.gui;

import com.craftaro.core.lootables.loot.Loot;
import com.craftaro.core.gui.Gui;

import java.util.List;

public class GuiLoreEditor extends AbstractGuiListEditor {
    public GuiLoreEditor(Loot loot, Gui returnGui) {
        super(loot, returnGui);
    }

    @Override
    protected List<String> getData() {
        return loot.getLore();
    }

    @Override
    protected void updateData(List<String> list) {
        loot.setLore(list);
    }

    @Override
    protected String validate(String line) {
        return line.trim();
    }
}